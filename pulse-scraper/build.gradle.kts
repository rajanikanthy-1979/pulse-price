plugins {
    kotlin("jvm")
    kotlin("plugin.spring")
    id("org.springframework.boot")
}

dependencies {
    implementation(project(":pulse-common"))
    implementation(project(":pulse-pipeline"))

    // Non-blocking WebClient & Reactive Redis
    implementation("org.springframework.boot:spring-boot-starter-webflux")
    implementation("org.springframework.boot:spring-boot-starter-data-redis-reactive")

    // Apache Kafka Producer
    implementation("org.springframework.kafka:spring-kafka")

    // HTML parsing & extraction
    implementation("org.jsoup:jsoup:1.18.1")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")

    // Headless Browser Automation (Playwright for Java)
    implementation("com.microsoft.playwright:playwright:1.47.0")

    // Testing: WireMock & Testcontainers
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("io.projectreactor:reactor-test")
    testImplementation("org.wiremock:wiremock-standalone:3.9.1")
    testImplementation("com.redis:testcontainers-redis:2.2.2")
    testImplementation("org.testcontainers:junit-jupiter:1.20.1")
}
