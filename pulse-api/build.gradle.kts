plugins {
    kotlin("jvm")
    kotlin("plugin.spring")
    id("org.springframework.boot")
    id("io.spring.dependency-management")
}

dependencies {
    implementation(project(":pulse-common"))

    // Reactive Web & Coroutines
    implementation("org.springframework.boot:spring-boot-starter-webflux")
    implementation("io.projectreactor.kotlin:reactor-kotlin-extensions")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor:1.8.1")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactive:1.8.1")

    // Reactive Database & TimescaleDB Data Access
    implementation("org.springframework.boot:spring-boot-starter-data-r2dbc")
    implementation("org.postgresql:r2dbc-postgresql:1.0.5.RELEASE")
    implementation("org.postgresql:postgresql:42.7.3") // JDBC driver for flyway/admin utilities if needed

    // Reactive Redis
    implementation("org.springframework.boot:spring-boot-starter-data-redis-reactive")

    // Apache Kafka Stream Consumer & Producer
    implementation("org.springframework.kafka:spring-kafka")

    // Validation & Utilities
    implementation("org.springframework.boot:spring-boot-starter-validation")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")

    // Actuator & Micrometer for Operational Health
    implementation("org.springframework.boot:spring-boot-starter-actuator")

    // Testing
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("io.projectreactor:reactor-test")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.8.1")
}

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
    compilerOptions {
        jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_21)
        freeCompilerArgs.add("-Xjsr305=strict")
    }
}
