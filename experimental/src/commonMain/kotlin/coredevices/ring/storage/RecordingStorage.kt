package coredevices.ring.storage

import co.touchlab.kermit.Logger
import coredevices.ring.audio.M4aDecoder
import coredevices.ring.audio.M4aEncoder
import coredevices.ring.data.entity.room.CachedRecordingMetadata
import coredevices.ring.database.room.dao.CachedRecordingMetadataDao
import coredevices.util.PrivacyPolicy
import coredevices.util.writeWavHeader
import dev.gitlive.firebase.Firebase
import dev.gitlive.firebase.auth.auth
import dev.gitlive.firebase.storage.File
import dev.gitlive.firebase.storage.FirebaseStorageMetadata
import dev.gitlive.firebase.storage.storage
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.IO
import kotlinx.coroutines.withContext
import kotlinx.io.Sink
import kotlinx.io.Source
import kotlinx.io.buffered
import kotlinx.io.files.Path
import kotlinx.io.files.SystemFileSystem
import kotlinx.io.readByteArray
import kotlinx.io.readShortLe
import kotlinx.io.writeShortLe

/**
 * Platform-specific path for caching recordings before they are persisted
 */
internal expect fun getRecordingsCacheDirectory(): Path

/**
 * Platform-specific path for storing complete recordings
 */
internal expect fun getRecordingsDataDirectory(): Path

expect fun getFirebaseStorageFile(path: Path): File

/**
 * Access storage for recordings
 */
interface RecordingStorage {

    fun getCacheDirectory(): Path

    /**
     * Export a recording to a WAV file
     * @param id unique identifier for the recording
     * @param useOriginalAudio export the original raw capture instead of the processed version
     * @return path to the exported file
     */
    suspend fun exportRecording(id: String, useOriginalAudio: Boolean = false): Path

    /**
     * Open a sink for writing recording data, storing temporarily in cache
     * until [commitLocalRecording] is called
     * @param id unique identifier for the recording, cannot contain characters that are invalid in file names
     */
    suspend fun openRecordingSink(id: String, sampleRate: Int, mimeType: String): Sink

    /**
     * Open a sink for writing the original raw version of a recording, storing temporarily in cache
     * until [commitLocalRecording] is called
     * @param id unique identifier for the recording, cannot contain characters that are invalid in file names
     */
    suspend fun openOriginalRecordingSink(id: String, sampleRate: Int, mimeType: String): Sink

    /**
     * Open a source for reading recording data
     */
    suspend fun openRecordingSource(idNoSuffix: String, useOriginalAudio: Boolean = false): Pair<Source, RecordingSourceInfo>

    /**
     * Open a source for reading recording data only from this device.
     */
    suspend fun openCachedRecordingSource(idNoSuffix: String, useOriginalAudio: Boolean = false): Pair<Source, RecordingSourceInfo>?

    /**
     * Encodes the processed working copy and commits it to durable app storage.
     * @param id unique identifier for the recording
     */
    suspend fun commitLocalRecording(id: String)

    /** Deletes transient PCM copies after a durable commit. */
    suspend fun cleanupWorkingRecording(id: String)

    suspend fun uploadRecordingPcm(
        id: String,
        sampleRate: Int,
        pcmBytes: ByteArray,
        encryptionKey: String?,
    )

    /**
     * Deletes a recording from persistent storage
     * @param id unique identifier for the recording
     */
    suspend fun deleteRecording(id: String)

    /**
     * Deletes a recording from cache
     * @param id unique identifier for the recording
     */
    fun deleteRecordingFromCache(id: String)

    /**
     * Check if a recording exists in storage, does not check cache
     * @param id unique identifier for the recording
     */
    fun recordingExists(id: String): Boolean

    /**
     * Delete all cached recording metadata from the database.
     */
    suspend fun deleteAllCachedMetadata()

    /**
     * Clear all files from the recordings cache directory.
     */
    fun clearCacheDirectory()

    /** Clear all durable recording files. */
    fun clearDataDirectory()

    /**
     * Delete a recording's audio file from Firebase Storage.
     */
    suspend fun deleteFromFirebaseStorage(id: String)

    /**
     * Information about a recording source returned by [openRecordingSource]
     * @param id ID used to obtain the source
     * @param cachedMetadata metadata for the recording
     * @param size size of the recording in bytes
     */
    data class RecordingSourceInfo(
        val id: String,
        val cachedMetadata: CachedRecordingMetadata,
        val size: Long,
    )
}

/**
 * Access storage for recordings
 */
class RealRecordingStorage(
    private val cachedMetadataDao: CachedRecordingMetadataDao,
    private val documentEncryptor: coredevices.ring.encryption.DocumentEncryptor,
) : RecordingStorage {
    companion object {
        private val logger = Logger.withTag(RealRecordingStorage::class.simpleName!!)
        private const val PCM_MIME = "audio/raw"
        private const val M4A_MIME = "audio/mp4"
        private const val DURABLE_SUFFIX = ".m4a"
        private const val STAGING_SUFFIX = ".tmp"
    }

    private val m4aEncoder = M4aEncoder()
    private val m4aDecoder = M4aDecoder()
    init {
        ensureDirectories() // Ensure full paths created on first access
    }
    private fun ensureDirectories() {
        val cache = getRecordingsCacheDirectory()
        val data = getRecordingsDataDirectory()
        SystemFileSystem.createDirectories(cache, false)
        SystemFileSystem.createDirectories(data, false)
    }

    override fun getCacheDirectory(): Path = getRecordingsCacheDirectory()

    override suspend fun exportRecording(id: String, useOriginalAudio: Boolean): Path = withContext(Dispatchers.IO) {
        val (source, meta) = openRecordingSource(id, useOriginalAudio)
        val suffix = if (useOriginalAudio) "-original" else ""
        val path = Path(getRecordingsCacheDirectory(), "share-$id$suffix.wav")
        source.use {
            SystemFileSystem.sink(path).buffered().use { sink ->
                sink.writeWavHeader(meta.cachedMetadata.sampleRate, meta.size.toInt())
                source.transferTo(sink)
            }
        }
        return@withContext path
    }

    override suspend fun openRecordingSink(id: String, sampleRate: Int, mimeType: String): Sink = withContext(Dispatchers.IO) {
        val metadata = CachedRecordingMetadata(id, sampleRate, mimeType)
        cachedMetadataDao.insertOrReplace(metadata)
        return@withContext SystemFileSystem.sink(Path(getRecordingsCacheDirectory(), id)).buffered()
    }

    override suspend fun openOriginalRecordingSink(id: String, sampleRate: Int, mimeType: String): Sink = withContext(Dispatchers.IO) {
        val metadata = CachedRecordingMetadata("$id-original", sampleRate, mimeType)
        cachedMetadataDao.insertOrReplace(metadata)
        return@withContext SystemFileSystem.sink(Path(getRecordingsCacheDirectory(), "$id-original")).buffered()
    }

    private fun durablePath(id: String): Path =
        Path(getRecordingsDataDirectory(), "$id$DURABLE_SUFFIX")

    private suspend fun getLocalCachedRecording(id: String): Pair<Path, RecordingStorage.RecordingSourceInfo> {
        val cachedPath = Path(getRecordingsCacheDirectory(), id)
        var cachedMetadata = cachedMetadataDao.get(id)
        return if (!SystemFileSystem.exists(cachedPath) || cachedMetadata == null) {
            val durablePath = durablePath(id)
            require(SystemFileSystem.exists(durablePath)) {
                "Recording $id is not available on this device"
            }
            logger.d { "Decoding durable recording $id" }
            val payload = SystemFileSystem.source(durablePath).buffered().use { it.readByteArray() }
            val decoded = m4aDecoder.decode(payload)
            require(decoded.samples.isNotEmpty()) { "Durable recording $id contains no audio" }
            SystemFileSystem.sink(cachedPath).buffered().use { sink ->
                for (sample in decoded.samples) sink.writeShortLe(sample)
            }
            cachedMetadata = CachedRecordingMetadata(id, decoded.sampleRate, PCM_MIME)
            cachedMetadataDao.insertOrReplace(cachedMetadata)
            val size = SystemFileSystem.metadataOrNull(cachedPath)?.size ?: error("Failed to get size of cached recording $id")
            Pair(cachedPath, RecordingStorage.RecordingSourceInfo(id, cachedMetadata, size))
        } else {
            logger.d { "Recording $id found in cache" }
            val size = SystemFileSystem.metadataOrNull(cachedPath)?.size ?: error("Failed to get size of cached recording $id")
            Pair(cachedPath, RecordingStorage.RecordingSourceInfo(id, cachedMetadata, size))
        }
    }

    override suspend fun openRecordingSource(idNoSuffix: String, useOriginalAudio: Boolean): Pair<Source, RecordingStorage.RecordingSourceInfo> = withContext(Dispatchers.IO) {
        try {
            val id = if (useOriginalAudio) "$idNoSuffix-original" else idNoSuffix
            val (path, info) = getLocalCachedRecording(id)
            return@withContext Pair(SystemFileSystem.source(path).buffered(), info)
        } catch (e: Exception) {
            if (useOriginalAudio) {
                logger.w(e) { "Failed to open original recording source for $idNoSuffix, falling back to processed version" }
                val (path, info) = getLocalCachedRecording(idNoSuffix)
                return@withContext Pair(SystemFileSystem.source(path).buffered(), info)
            } else {
                logger.w(e) { "Failed to open recording source for $idNoSuffix, falling back to original version" }
                val (path, info) = getLocalCachedRecording("$idNoSuffix-original")
                return@withContext Pair(SystemFileSystem.source(path).buffered(), info)
            }
        }
    }

    override suspend fun openCachedRecordingSource(idNoSuffix: String, useOriginalAudio: Boolean): Pair<Source, RecordingStorage.RecordingSourceInfo>? = withContext(Dispatchers.IO) {
        val id = if (useOriginalAudio) "$idNoSuffix-original" else idNoSuffix
        return@withContext try {
            val (path, info) = getLocalCachedRecording(id)
            Pair(SystemFileSystem.source(path).buffered(), info)
        } catch (_: Exception) {
            null
        }
    }

    override suspend fun commitLocalRecording(id: String) = withContext(Dispatchers.IO) {
        val destination = durablePath(id)
        if (SystemFileSystem.exists(destination)) return@withContext

        val source = Path(getRecordingsCacheDirectory(), id)
        val metadata = cachedMetadataDao.get(id)
            ?: error("Cached metadata for recording $id not found")
        require(SystemFileSystem.exists(source)) {
            "Recording $id does not exist in the working directory"
        }

        val encoded = m4aEncoder.encode(readPcmFile(source), metadata.sampleRate)
        val decoded = m4aDecoder.decode(encoded)
        require(decoded.samples.isNotEmpty()) { "Encoded recording $id contains no audio" }
        require(decoded.sampleRate == metadata.sampleRate) {
            "Encoded recording $id changed sample rate from ${metadata.sampleRate} to ${decoded.sampleRate}"
        }

        val staging = Path(getRecordingsDataDirectory(), "$id$DURABLE_SUFFIX$STAGING_SUFFIX")
        try {
            SystemFileSystem.sink(staging).buffered().use { it.write(encoded) }
            if (SystemFileSystem.exists(destination)) {
                SystemFileSystem.delete(destination)
            }
            SystemFileSystem.atomicMove(staging, destination)
            logger.i { "Committed recording $id to durable local storage" }
            deleteWorkingFile("$id-original")
        } finally {
            if (SystemFileSystem.exists(staging)) SystemFileSystem.delete(staging)
        }
    }

    override suspend fun cleanupWorkingRecording(id: String) = withContext(Dispatchers.IO) {
        deleteWorkingFile(id, keepMetadata = true)
        deleteWorkingFile("$id-original")
    }

    private suspend fun deleteWorkingFile(id: String, keepMetadata: Boolean = false) {
        val path = Path(getRecordingsCacheDirectory(), id)
        if (SystemFileSystem.exists(path)) SystemFileSystem.delete(path)
        if (!keepMetadata) cachedMetadataDao.delete(id)
    }

    override suspend fun uploadRecordingPcm(
        id: String,
        sampleRate: Int,
        pcmBytes: ByteArray,
        encryptionKey: String?,
    ) = withContext(Dispatchers.IO) {
        check(PrivacyPolicy.CLOUD_SERVICES_ENABLED) { "Cloud storage is disabled" }
        uploadRecordingSamples(
            id = id,
            sampleRate = sampleRate,
            samples = withContext(Dispatchers.Default) { pcmBytesToShortArray(pcmBytes) },
            encryptionKey = encryptionKey,
        )
    }

    /**
     * Read a raw PCM 16-bit little-endian mono file into a ShortArray.
     */
    private fun readPcmFile(path: Path): ShortArray {
        val size = SystemFileSystem.metadataOrNull(path)?.size
            ?: error("Failed to get size of recording at $path")
        val numSamples = (size / 2).toInt()
        val samples = ShortArray(numSamples)
        SystemFileSystem.source(path).buffered().use { src ->
            for (i in 0 until numSamples) {
                samples[i] = src.readShortLe()
            }
        }
        return samples
    }

    private fun pcmBytesToShortArray(bytes: ByteArray): ShortArray {
        require(bytes.size % 2 == 0) { "PCM byte array must contain 16-bit samples" }
        val samples = ShortArray(bytes.size / 2)
        var sampleIndex = 0
        var byteIndex = 0
        while (byteIndex < bytes.size) {
            val lo = bytes[byteIndex].toInt() and 0xFF
            val hi = bytes[byteIndex + 1].toInt()
            samples[sampleIndex] = ((hi shl 8) or lo).toShort()
            sampleIndex++
            byteIndex += 2
        }
        return samples
    }

    private suspend fun uploadRecordingSamples(
        id: String,
        sampleRate: Int,
        samples: ShortArray,
        encryptionKey: String?,
    ) {
        check(PrivacyPolicy.CLOUD_SERVICES_ENABLED) { "Cloud storage is disabled" }
        val destination = "recordings/${Firebase.auth.currentUser!!.uid}/$id"
        val m4aBytes = m4aEncoder.encode(samples, sampleRate)
        val uploadBytes = if (encryptionKey != null) {
            documentEncryptor.encryptAudio(m4aBytes, encryptionKey)
        } else {
            m4aBytes
        }

        val m4aTempPath = Path(getRecordingsCacheDirectory(), "$id.upload.m4a")
        SystemFileSystem.sink(m4aTempPath).buffered().use { it.write(uploadBytes) }

        val customMeta = mutableMapOf(
            "sampleRate" to sampleRate.toString()
        )
        if (encryptionKey != null) {
            customMeta["encrypted"] = "true"
            customMeta["keyFingerprint"] =
                coredevices.ring.encryption.AesCbcHmacCrypto.keyFingerprint(encryptionKey)
        }

        try {
            Firebase.storage.reference(destination)
                .putFile(
                    getFirebaseStorageFile(m4aTempPath),
                    FirebaseStorageMetadata(
                        contentType = M4A_MIME,
                        customMetadata = customMeta
                    )
                )
        } finally {
            if (SystemFileSystem.exists(m4aTempPath)) {
                SystemFileSystem.delete(m4aTempPath)
            }
        }
    }

    override suspend fun deleteRecording(id: String) = withContext(Dispatchers.IO) {
        listOf(
            durablePath(id),
            Path(getRecordingsDataDirectory(), "$id$DURABLE_SUFFIX$STAGING_SUFFIX"),
            Path(getRecordingsCacheDirectory(), id),
            Path(getRecordingsCacheDirectory(), "$id-original"),
        ).forEach { path ->
            if (SystemFileSystem.exists(path)) SystemFileSystem.delete(path)
        }
        cachedMetadataDao.delete(id)
        cachedMetadataDao.delete("$id-original")
    }

    override fun deleteRecordingFromCache(id: String) {
        val source = Path(getRecordingsCacheDirectory(), id)
        SystemFileSystem.delete(source)
    }

    override fun recordingExists(id: String): Boolean {
        return SystemFileSystem.exists(durablePath(id))
    }

    override suspend fun deleteAllCachedMetadata() {
        cachedMetadataDao.deleteAll()
        logger.i { "Deleted all cached recording metadata" }
    }

    override fun clearCacheDirectory() {
        val cacheDir = getRecordingsCacheDirectory()
        try {
            val entries = SystemFileSystem.list(cacheDir)
            for (entry in entries) {
                try {
                    SystemFileSystem.delete(entry, false)
                } catch (_: Exception) { }
            }
            logger.i { "Cleared ${entries.size} files from cache directory" }
        } catch (e: Exception) {
            logger.w { "Failed to clear cache directory: ${e.message}" }
        }
    }

    override fun clearDataDirectory() {
        val dataDir = getRecordingsDataDirectory()
        try {
            val entries = SystemFileSystem.list(dataDir)
            for (entry in entries) {
                try {
                    SystemFileSystem.delete(entry, false)
                } catch (_: Exception) { }
            }
            logger.i { "Cleared ${entries.size} durable recording files" }
        } catch (e: Exception) {
            logger.w { "Failed to clear durable recording directory: ${e.message}" }
        }
    }

    override suspend fun deleteFromFirebaseStorage(id: String) {
        if (!PrivacyPolicy.CLOUD_SERVICES_ENABLED) return
        val path = "recordings/${Firebase.auth.currentUser!!.uid}/$id"
        try {
            Firebase.storage.reference(path).delete()
        } catch (e: Exception) {
            logger.w { "Failed to delete Storage file $id: ${e.message}" }
        }
    }

}
