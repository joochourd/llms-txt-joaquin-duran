package com.joaquinduran.llmstxt

import com.joaquinduran.llmstxt.bootstrap.LocalBootstrapCommand
import com.joaquinduran.llmstxt.bootstrap.ProductionBootstrapCommand
import com.joaquinduran.llmstxt.bootstrap.ReadinessProbe
import io.micronaut.runtime.Micronaut
import kotlin.system.exitProcess

fun main(args: Array<String>) {
    when (args.firstOrNull()) {
        "--init-local" -> LocalBootstrapCommand.execute()
        "--init-aws" -> ProductionBootstrapCommand.execute()
        "--probe" -> exitProcess(ReadinessProbe.exitCode())
        else -> Micronaut.run(*args)
    }
}
