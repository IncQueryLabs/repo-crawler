[![Build Status](https://build.incquerylabs.com/jenkins/buildStatus/icon?job=incquery-suite%2Ftwc-repo-crawler%2Fmaster)](https://build.incquerylabs.com/jenkins/job/incquery-suite/job/twc-repo-crawler/job/master/)

# Repository and revision crawler for Teamwork Cloud

## Run the crawler

1. Download `twc.repo-crawler-<version>-all.jar` from [GitHub releases](https://github.com/IncQueryLabs/repo-crawler/releases)
1. `java -jar twc.repo-crawler-<version>-all.jar <args>` with arguments (see below)

## Example arguments

Running the jar without arguments prints out the usage instructions.

- Running the crawler without workspace, resource, branch, and revision will result in crawling the repository structure of the given scope
- Running the crawler with workspace, resource, branch, and revision will result in crawling the model content of the given revision

### Repository structure of OpenSE Cookbook workspace on twc.openmbee.org

`-S twc.openmbee.org -P 8111 -ssl -W 9c368adc-10cc-45d9-bec6-27aedc80e68b -C 2000 -u openmbeeguest -pw guest`

### OpenSE Cookbook model on twc.openmbee.org

`-S twc.openmbee.org -P 8111 -ssl -W 9c368adc-10cc-45d9-bec6-27aedc80e68b -R c6bede89-cd5e-487b-aca8-a4f384370759 -B 29110c0f-bdc1-4294-ae24-9fd608629cac -REV 350 -C 2000 -u openmbeeguest -pw guest`

### TMT on twc.openmbee.org

`-S twc.openmbee.org -P 8111 -ssl -W 9c368adc-10cc-45d9-bec6-27aedc80e68b -R 6819171d-1f52-4792-a08d-15d50d47985a -B a95e8bd1-f7d2-433e-a0a1-0c1cd7702e59 -REV 229 -C 2000 -u openmbeeguest -pw guest`

## Configuring self-signed certificate when using SSL on Teamwork Cloud OSMC API

- Java has to start with `-Djavax.net.ssl.trustStore=<path to JKS file> -Djavax.net.ssl.trustStorePassword=<password of JKS file>`
  - **Note**: These are JVM parameters that should be added before the `-jar` part
  - You can add `-Djavax.net.debug=all` to check whether Java loaded the certificate correctly
- Make sure that `ssl` is enabled in CLI arguments

## Bypass host verification and trust all certificates by default

At your own risk you can use the `--trustAll` option to disable validation.

## Running from source

1. Clone this repository or download the sources for a [release](https://github.com/IncQueryLabs/repo-crawler/releases).

### IntelliJ IDEA

1. Install `IntelliJ IDEA` with Kotlin plugin.
1. Open IntelliJ IDEA and open project. `File -> Open.. -> <project_path>` (Select `Gradle` if needed.)
1. `Build -> Build Project`
1. `Run -> Run... -> com.incquerylabs.twc.repo.crawler.Crawler.kt` (set arguments in Configuration)

### Gradle

1. `./gradlew run --args='<args>'` with arguments (see above)

### Shadow Jar

1. `./gradlew shadowJar`
1. `java -jar build/libs/twc.repo-crawler-<version>-all.jar <args>` with arguments (see above)
