# Repository and revision crawler for Teamwork Cloud 19.0

## Run the crawler

1. Download `vhci.modelaccess.twc.rest-<version>-all.jar` from [GitHub releases](https://github.com/IncQueryLabs/repo-crawler/releases)
1. `java -jar vhci.modelaccess.twc.rest-<version>-all.jar <args>` with arguments (see below)

## Example arguments

Running the jar without arguments prints out the usage instructions.

### OpenSE Cookbook model on twc.openmbee.org

`-S twc.openmbee.org -P 8111 -ssl -W 9c368adc-10cc-45d9-bec6-27aedc80e68b -R c6bede89-cd5e-487b-aca8-a4f384370759 -B 29110c0f-bdc1-4294-ae24-9fd608629cac -REV 350 -C 2000 -u openmbeeguest -pw guest`

### TMT on twc.openmbee.org

`-S twc.openmbee.org -P 8111 -ssl -W 9c368adc-10cc-45d9-bec6-27aedc80e68b -R 6819171d-1f52-4792-a08d-15d50d47985a -B a95e8bd1-f7d2-433e-a0a1-0c1cd7702e59 -REV 229 -C 2000 -u openmbeeguest -pw guest`

## Running from source

1. Clone this repository or download the sources for a [release](https://github.com/IncQueryLabs/repo-crawler/releases).

### IntelliJ IDEA

1. Install `IntelliJ IDEA` with kotlin plugin.
1. Open IntelliJ IDEA and open project. `File -> Open.. -> <project_path>` (Select `Gradle` if needed.)
1. `Build -> Build Project`
1. `Run -> Run... -> com.incquerylabs.vhci.modelaccess.twc.rest.Crawler.kt` (set arguments in Configuration)

### Gradle

1. `./gradlew run --args='<args>'` with arguments (see above)

### Shadow Jar

1. `./gradlew shadowJar`
1. `java -jar build/libs/vhci.modelaccess.twc.rest-<version>-all.jar <args>` with arguments (see above)
