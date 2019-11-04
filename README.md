# repo-crawler

## Getting Started
The easiest way to set up the environment is to:
1. Install `IntelliJ IDEA` with kotlin plugin.
2. Clone this repository.
3. Open IntelliJ IDEA and open project. `File -> Open.. -> <project_path>` (Select `Gradle` if needed.)
4. `Build -> Build Project`
5. `Run -> Run... -> com.incquerylabs.vhci.modelaccess.twc.rest.Crawler.kt`

## Example arguments

### OpenSE Cookbook model on twc.openmbee.org

-S twc.openmbee.org -P 8111 -ssl -W 9c368adc-10cc-45d9-bec6-27aedc80e68b -R c6bede89-cd5e-487b-aca8-a4f384370759 -B 29110c0f-bdc1-4294-ae24-9fd608629cac -REV 350 -C 2000 openmbeeguest guest

### TMT on twc.openmbee.org

-S twc.openmbee.org -P 8111 -ssl -W 9c368adc-10cc-45d9-bec6-27aedc80e68b -R 6819171d-1f52-4792-a08d-15d50d47985a -B a95e8bd1-f7d2-433e-a0a1-0c1cd7702e59 -REV 229 -C 2000 openmbeeguest guest