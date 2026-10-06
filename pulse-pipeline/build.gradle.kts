plugins {
    kotlin("jvm")
    kotlin("plugin.spring")
    id("org.springframework.boot")
}

dependencies {
    implementation(project(":pulse-common"))

    // Reactive WebFlux
    implementation("org.springframework.boot:spring-boot-starter-webflux")

    // Apache Kafka Consumer & Producer
    implementation("org.springframework.kafka:spring-kafka")

    // Reactive Database with R2DBC & PostgreSQL Driver
    implementation("org.springframework.boot:spring-boot-starter-data-r2dbc")
    implementation("org.postgresql:r2dbc-postgresql:1.0.5.RELEASE")

    // Reactive Redis for GTIN Canonical Resolution Cache
    implementation("org.springframework.boot:spring-boot-starter-data-redis-reactive")

    // JSON serialization
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")
}
