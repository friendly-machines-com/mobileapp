package coredevices.util

/**
 * Capabilities intentionally disabled in the Android privacy fork.
 *
 * Keeping these decisions in one place makes accidental cloud or telemetry use visible and
 * gives network-facing services a common fail-closed guard.
 */
object PrivacyPolicy {
    const val CLOUD_SERVICES_ENABLED = false
    const val REMOTE_INDEX_PROCESSING_ENABLED = false
    const val TELEMETRY_ENABLED = false
    const val PUSH_MESSAGING_ENABLED = false
}
