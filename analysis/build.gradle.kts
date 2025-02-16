plugins {
    id("application")
}

dependencies {
    implementation(project(":client")) // Import moduleA
    implementation(project(":server")) // Import moduleA
}

application {
    mainClass.set("se.edinjakupovic.Main")
}

tasks.jar {
    manifest {
        attributes["Main-class"] = application.mainClass.get()
    }
    from(configurations.runtimeClasspath.get().filter { it.isDirectory }.map { it } +
            configurations.runtimeClasspath.get().filter { !it.isDirectory }.map { zipTree(it) })
}