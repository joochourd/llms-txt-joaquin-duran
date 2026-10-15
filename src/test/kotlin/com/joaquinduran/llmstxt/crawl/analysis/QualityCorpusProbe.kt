package com.joaquinduran.llmstxt.crawl.analysis

import com.joaquinduran.llmstxt.crawl.configuration.CrawlConfiguration
import com.joaquinduran.llmstxt.crawl.generation.LlmsRenderer
import com.joaquinduran.llmstxt.crawl.model.Candidate
import java.nio.file.Files
import java.nio.file.Path

/** No external requests: exports the same invented fixtures exercised by the quality tests. */
fun main(args: Array<String>) {
    require(args.size == 1) { "Provide an output directory." }
    val output = Files.createDirectories(Path.of(args.single()))
    val analyzer = DeterministicAnalyzer()
    val renderer = LlmsRenderer(CrawlConfiguration())
    for (name in listOf("documentation", "news", "marketing", "developer")) {
        val analysis = QualityFixtures.analysis(name)
        val selected = analyzer.select(analysis.resources, 8, analysis.families)
        val (bytes, rendered) = renderer.render(analysis.seed, analysis.context, selected)
        Files.write(output.resolve("$name.llms.txt"), bytes)
        Files.writeString(
            output.resolve("$name.analysis.txt"),
            buildString {
                appendLine(analysis.context)
                analysis.resources.sortedBy { it.resourceKey }.forEach { resource ->
                    appendLine(resource)
                    appendLine("family=${analysis.families.getValue(resource.resourceKey)}")
                }
                appendLine("selected=${selected.map { it.resourceKey }}")
                appendLine("rendered=${rendered.map { it.resourceKey }}")
            },
        )
    }
    val shell = QualityFixtures.extract(
        "/",
        QualityFixtures.html("insufficient"),
        Candidate("https://quality.example/", "SEED", null, "", 0, 0.0, seed = true),
    )
    Files.writeString(
        output.resolve("insufficient.analysis.txt"),
        "useful=${analyzer.useful(shell)}\ntextLength=${shell.textLength}\nlinks=${shell.links.size}\njavascriptShell=${shell.javascriptShell}\n",
    )
    println("Exported five deterministic corpus shapes to $output.")
}
