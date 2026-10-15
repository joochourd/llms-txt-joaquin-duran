package com.joaquinduran.llmstxt.monitoring

data class FixturePage(
    val status: Int,
    val contentType: String,
    val body: String,
)

/** Time changes only the source. The scheduler independently discovers and validates those changes. */
object MonitoringSourceFixture {
    const val ROBOTS = "User-agent: *\nAllow: /\n"
    private const val FILE_CHANGE_AFTER_MILLIS = 45_000L
    private const val CONTENT_CHANGE_AFTER_MILLIS = 90_000L

    fun page(
        path: String,
        elapsed: Long,
    ): FixturePage {
        val changed = elapsed >= FILE_CHANGE_AFTER_MILLIS
        val bodyChanged = elapsed >= CONTENT_CHANGE_AFTER_MILLIS
        val base = "/"
        val html = when (path) {
            "" -> {
                val description = if (changed) {
                    "Plan projects and share progress with your team."
                } else {
                    "Plan projects and organize work with your team."
                }
                val destination = if (changed) "guide" else "help"
                val label = if (changed) "Getting started guide" else "Help center"
                document(
                    "Orbit workspace",
                    description,
                    "Practical project planning for small teams. ".repeat(12),
                    "<nav><a href='${base}product'>Product overview</a><a href='$base$destination'>$label</a></nav>",
                )
            }

            "product" -> {
                document(
                    "Project planning",
                    "Features for planning and tracking team projects.",
                    (
                        if (bodyChanged) {
                            "Track tasks, priorities and milestones for each project. "
                        } else {
                            "Plan tasks, priorities and milestones for each project. "
                        }
                    ).repeat(12),
                )
            }

            "help" -> {
                if (changed) {
                    return FixturePage(404, "text/plain", "The help page has been replaced by the guide.")
                } else {
                    document(
                        "Help center",
                        "Answers about workspace setup and project planning.",
                        "Find answers about workspace setup and team collaboration. ".repeat(12),
                    )
                }
            }

            "guide" -> {
                if (!changed) {
                    return FixturePage(404, "text/plain", "Page not found.")
                } else {
                    document(
                        "Getting started guide",
                        "Create your workspace and plan your first project.",
                        "Create a workspace, invite your team and organize the first project. ".repeat(12),
                    )
                }
            }

            else -> {
                return FixturePage(404, "text/plain", "Page not found.")
            }
        }
        return FixturePage(200, "text/html", html)
    }

    private fun document(
        title: String,
        description: String,
        body: String,
        navigation: String = "",
    ): String = "<!doctype html><html lang='en'><head><meta charset='utf-8'><title>$title</title>" +
        "<meta name='description' content='$description'></head><body><h1>$title</h1>$navigation<main><p>$body</p></main></body></html>"
}
