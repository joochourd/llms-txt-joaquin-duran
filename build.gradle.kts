import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    kotlin("jvm") version "2.3.21"
    kotlin("plugin.allopen") version "2.3.21"
    id("com.google.devtools.ksp") version "2.3.7"
    application
}

group = "com.joaquinduran.llmstxt"
version = "0.1.0"

repositories { mavenCentral() }

val micronautVersion = "5.1.5"
val awsSdkVersion = "2.54.3"
val kotlinStyle by configurations.creating

dependencies {
    kotlinStyle("com.pinterest.ktlint:ktlint-cli:1.8.0:all")
    implementation(platform("io.micronaut.platform:micronaut-platform:$micronautVersion"))
    ksp(platform("io.micronaut.platform:micronaut-platform:$micronautVersion"))
    ksp("io.micronaut:micronaut-inject-kotlin")
    ksp("io.micronaut.serde:micronaut-serde-processor")
    implementation("io.micronaut:micronaut-http-server-netty")
    implementation("io.micronaut.serde:micronaut-serde-jackson")
    implementation("io.micronaut.kotlin:micronaut-kotlin-runtime")
    runtimeOnly("ch.qos.logback:logback-classic")

    implementation(platform("software.amazon.awssdk:bom:$awsSdkVersion"))
    implementation("software.amazon.awssdk:dynamodb")
    implementation("software.amazon.awssdk:sso")
    implementation("software.amazon.awssdk:ssooidc")
    implementation("software.amazon.awssdk:sts")
    implementation("software.amazon.awssdk:url-connection-client")
    implementation("com.squareup.okhttp3:okhttp:5.3.2")
    implementation("org.jsoup:jsoup:1.22.2")
    implementation("org.commonmark:commonmark:0.30.1")
    implementation("tools.jackson.core:jackson-core")
    implementation("com.github.crawler-commons:crawler-commons:1.6")

    // Pin the existing JUnit 5 suite independently of Micronaut platform upgrades.
    testImplementation(enforcedPlatform("org.junit:junit-bom:5.13.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

java { toolchain { languageVersion = JavaLanguageVersion.of(25) } }
kotlin {
    jvmToolchain(25)
    compilerOptions {
        jvmTarget = JvmTarget.JVM_25
        freeCompilerArgs.add("-Xjsr305=strict")
    }
}
allOpen { annotation("io.micronaut.http.annotation.Controller") }
ksp { arg("micronaut.processing.annotations", "com.joaquinduran.llmstxt.*") }

application { mainClass = "com.joaquinduran.llmstxt.ApplicationKt" }

tasks.register<JavaExec>("checkKotlinStyle") {
    description = "Checks Kotlin formatting against the shared EditorConfig conventions."
    group = "verification"
    classpath = kotlinStyle
    mainClass = "com.pinterest.ktlint.Main"
    args("src/**/*.kt", "*.kts")
}

tasks.register<JavaExec>("formatKotlin") {
    description = "Formats Kotlin source, tests and build scripts using the shared conventions."
    group = "formatting"
    classpath = kotlinStyle
    mainClass = "com.pinterest.ktlint.Main"
    args("--format", "src/**/*.kt", "*.kts")
}

tasks.named("check") {
    dependsOn("checkKotlinStyle")
}

tasks.withType<Test>().configureEach {
    testLogging { exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL }
}

tasks.test {
    useJUnitPlatform { excludeTags("integration") }
}

tasks.register<Test>("integrationTest") {
    description = "Runs SDK and HTTP integration tests against an explicitly configured DynamoDB Local."
    group = "verification"
    dependsOn(tasks.testClasses)
    testClassesDirs =
        sourceSets.test
            .get()
            .output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform { includeTags("integration") }
    shouldRunAfter(tasks.test)
}

tasks.register<JavaExec>("restartProbe") {
    description = "Writes, verifies, or removes the local M0 restart marker."
    group = "verification"
    dependsOn(tasks.testClasses)
    classpath = sourceSets.test.get().runtimeClasspath
    mainClass = "com.joaquinduran.llmstxt.persistence.dynamo.RestartProbeKt"
}

tasks.register<JavaExec>("admissionRestartProbe") {
    description = "Verifies M1 admission persistence across a local container restart in an isolated table."
    group = "verification"
    dependsOn(tasks.testClasses)
    classpath = sourceSets.test.get().runtimeClasspath
    mainClass = "com.joaquinduran.llmstxt.persistence.dynamo.AdmissionRestartProbeKt"
}

tasks.register<JavaExec>("bulkRestartProbe") {
    description = "Verifies durable bulk inputs and admission progress across a local container restart."
    group = "verification"
    dependsOn(tasks.testClasses)
    classpath = sourceSets.test.get().runtimeClasspath
    mainClass = "com.joaquinduran.llmstxt.persistence.dynamo.BulkRestartProbeKt"
}

tasks.register<JavaExec>("crawlRestartProbe") {
    description = "Verifies M2 published current results across a local container restart."
    group = "verification"
    dependsOn(tasks.testClasses)
    classpath = sourceSets.test.get().runtimeClasspath
    mainClass = "com.joaquinduran.llmstxt.persistence.dynamo.CrawlRestartProbeKt"
}

tasks.register<JavaExec>("qualityProbe") {
    description = "Explicit manual crawl evaluation with opt-in diagnostics; never downloads external sites during tests."
    group = "verification"
    dependsOn(tasks.testClasses)
    classpath = sourceSets.test.get().runtimeClasspath
    mainClass = "com.joaquinduran.llmstxt.crawl.analysis.QualityProbeKt"
}

tasks.register<JavaExec>("qualityCorpusProbe") {
    description = "Exports deterministic corpus scores, selections and rendered bytes for comparison."
    group = "verification"
    dependsOn(tasks.testClasses)
    classpath = sourceSets.test.get().runtimeClasspath
    mainClass = "com.joaquinduran.llmstxt.crawl.analysis.QualityCorpusProbeKt"
}

tasks.register<JavaExec>("monitoringRestartProbe") {
    description = "Verifies durable monitoring and pending work across a local container restart."
    group = "verification"
    dependsOn(tasks.testClasses)
    classpath = sourceSets.test.get().runtimeClasspath
    mainClass = "com.joaquinduran.llmstxt.monitoring.MonitoringRestartProbeKt"
}

tasks.register<JavaExec>("monitoringUpdateProbe") {
    description = "Proves automatic updates through an isolated local application with test-only source transport."
    group = "verification"
    dependsOn(tasks.testClasses)
    classpath = sourceSets.test.get().runtimeClasspath
    mainClass = "com.joaquinduran.llmstxt.monitoring.MonitoringUpdateProbeKt"
}

tasks.register<JavaExec>("crawlLoadProbe") {
    description = "Measures 100 admitted synthetic crawls with four or eight workers in an isolated local table."
    group = "verification"
    dependsOn(tasks.testClasses)
    classpath = sourceSets.test.get().runtimeClasspath
    mainClass = "com.joaquinduran.llmstxt.crawl.execution.load.CrawlLoadProbeKt"
    jvmArgs("-Xmx384m", "-Dfile.encoding=UTF-8")
}

// Build Vite separately, then package its output into the application distribution.
val frontendAssets = layout.projectDirectory.dir("frontend/dist")
tasks.processResources {
    from(frontendAssets) { into("public") }
}
tasks.named("installDist") {
    doFirst {
        check(frontendAssets.file("index.html").asFile.isFile) {
            "Frontend assets are missing. Run npm ci && npm run build in frontend/ first."
        }
    }
}
