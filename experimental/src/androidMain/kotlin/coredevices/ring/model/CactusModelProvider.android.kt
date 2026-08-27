package coredevices.ring.model

import android.content.Context
import co.touchlab.kermit.Logger
import com.cactus.cactusSetTelemetryEnvironment
import coredevices.util.CommonBuildKonfig
import coredevices.util.models.promoteSingleRootDir
import kotlinx.io.files.Path
import org.koin.mp.KoinPlatform
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import java.io.File
import java.io.FileOutputStream
import java.util.concurrent.ConcurrentHashMap
import java.util.zip.ZipInputStream

actual class CactusModelProvider actual constructor() : coredevices.util.transcription.CactusModelPathProvider {
    companion object {
        private val logger = Logger.withTag("CactusModelProvider")
        private const val QUANTIZATION = "cq4"
        private const val MIN_FREE_SPACE_BYTES = 900L * 1024L * 1024L

        // One mutex per model so an in-progress STT download doesn't head-of-line
        // block an unrelated LM resolve (or vice versa).
        private val modelMutexes = ConcurrentHashMap<String, Mutex>()
        private fun mutexFor(modelName: String): Mutex =
            modelMutexes.getOrPut(modelName) { Mutex() }
    }

    private val context: Context get() = KoinPlatform.getKoin().get()
    private val modelsDir: File get() = context.filesDir.resolve("models").also { it.mkdirs() }

    actual override suspend fun getSTTModelPath(): String = withContext(Dispatchers.IO) {
        val modelName = CommonBuildKonfig.CACTUS_STT_MODEL
        return@withContext resolveModelPath(modelName, CommonBuildKonfig.CACTUS_WEIGHTS_VERSION)
    }

    actual override suspend fun getLMModelPath(): String = withContext(Dispatchers.IO) {
        val modelName = CommonBuildKonfig.CACTUS_LM_MODEL_NAME
        return@withContext resolveModelPath(modelName, CommonBuildKonfig.CACTUS_WEIGHTS_VERSION)
    }

    actual override fun isModelDownloaded(modelName: String): Boolean {
        val modelDir = modelsDir.resolve(modelName)
        return modelDir.exists() && modelDir.resolve("config.txt").exists()
    }

    actual override fun getDownloadedModels(): List<String> {
        return modelsDir.listFiles()
            ?.filter { it.isDirectory && it.resolve("config.txt").exists() }
            ?.map { it.name }
            ?: emptyList()
    }

    actual override fun getIncompatibleModels(): List<String> {
        val compatible = setOf(CommonBuildKonfig.CACTUS_STT_MODEL, CommonBuildKonfig.CACTUS_LM_MODEL_NAME)
        return getDownloadedModels().filter { name ->
            modelNeedsReplacement(name, compatible, versionMatches(name), isBundled(name))
        }
    }

    private fun versionMatches(modelName: String): Boolean {
        val versionFile = modelsDir.resolve(modelName).resolve(".cactus_version")
        return versionFile.exists() &&
            versionFile.readText().trim() == CommonBuildKonfig.CACTUS_WEIGHTS_VERSION
    }

    private fun isBundled(modelName: String): Boolean =
        context.assets.list("models")?.contains("${modelName.lowercase()}-$QUANTIZATION.zip") == true

    actual override fun deleteModel(modelName: String) {
        modelsDir.resolve(modelName).deleteRecursively()
    }

    actual override fun getModelSizeBytes(modelName: String): Long {
        val dir = modelsDir.resolve(modelName)
        return if (dir.exists()) dir.walkTopDown().sumOf { it.length() } else 0L
    }

    private suspend fun resolveModelPath(modelName: String, version: String): String = mutexFor(modelName).withLock {
        val modelDir = modelsDir.resolve(modelName)
        val versionFile = modelDir.resolve(".cactus_version")

        val needsDownload = !modelDir.exists()
            || !modelDir.resolve("config.txt").exists()
            || !versionFile.exists()
            || versionFile.readText().trim() != version

        if (needsDownload) {
            downloadAndExtract(modelName, modelDir, version)
            versionFile.writeText(version)
        }

        logger.d { "Model '$modelName' at: ${modelDir.absolutePath}" }
        return modelDir.absolutePath
    }

    private suspend fun downloadAndExtract(modelName: String, targetDir: File, version: String) = withContext(Dispatchers.IO) {
        val zipName = "${modelName.lowercase()}-$QUANTIZATION.zip"
        check(isBundled(modelName)) {
            "Required bundled model is missing from this build: models/$zipName"
        }
        check(modelsDir.usableSpace >= MIN_FREE_SPACE_BYTES) {
            "At least 900 MB of free app storage is required to prepare local speech"
        }

        val stagingDir = modelsDir.resolve(".$modelName.staging")
        try {
            stagingDir.deleteRecursively()
            stagingDir.mkdirs()

            logger.i { "Extracting included model directly from assets: $zipName" }
            ZipInputStream(context.assets.open("models/$zipName").buffered()).use { zis ->
                var entry = zis.nextEntry
                while (entry != null) {
                    currentCoroutineContext().ensureActive()
                    val outputFile = File(stagingDir, entry.name)
                    // ZIP Slip protection
                    val stagingPrefix = stagingDir.canonicalPath + File.separator
                    if (!outputFile.canonicalPath.startsWith(stagingPrefix)) {
                        throw SecurityException("ZIP entry outside target dir: ${entry.name}")
                    }
                    if (entry.isDirectory) {
                        outputFile.mkdirs()
                    } else {
                        outputFile.parentFile?.mkdirs()
                        FileOutputStream(outputFile).use { fos ->
                            zis.copyTo(fos)
                        }
                    }
                    zis.closeEntry()
                    entry = zis.nextEntry
                }
            }
            promoteSingleRootDir(Path(stagingDir.absolutePath))
            check(stagingDir.resolve("config.txt").isFile) {
                "Bundled model $modelName is invalid: config.txt is missing"
            }

            targetDir.deleteRecursively()
            check(stagingDir.renameTo(targetDir)) {
                "Could not atomically install bundled model $modelName"
            }
            logger.i { "Extraction complete to ${targetDir.absolutePath}" }
        } catch (e: CancellationException) {
            logger.i { "Model extraction cancelled for $modelName" }
            stagingDir.deleteRecursively()
            throw e
        } catch (e: Exception) {
            currentCoroutineContext().ensureActive()
            logger.e(e) { "Bundled model extraction failed for $modelName" }
            stagingDir.deleteRecursively()
            throw e
        }
    }

    actual fun setCloudApiKey(key: String) {
        val cacheDir = getCactusCacheDir()
        val keyFile = File(cacheDir, "cloud_api_key")
        keyFile.writeText(key)
        logger.d { "Cloud API key written to ${keyFile.absolutePath}" }
    }

    actual override fun initTelemetry() {
        val cacheDir = getCactusCacheDir()
        try {
            cactusSetTelemetryEnvironment("kotlin", cacheDir.absolutePath, null)
            logger.d { "Telemetry environment set to ${cacheDir.absolutePath}" }
        } catch (e: Throwable) {
            logger.e(e) { "Failed to initialize telemetry environment" }
        }
    }

    private fun getCactusCacheDir(): File {
        val dir = context.cacheDir.resolve("cactus")
        if (!dir.exists()) dir.mkdirs()
        return dir
    }
}
