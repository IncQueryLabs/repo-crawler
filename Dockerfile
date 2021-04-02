FROM gradle:5.6.1-jdk8 AS build
COPY --chown=gradle:gradle . /home/gradle/src
WORKDIR /home/gradle/src
RUN gradle clean build --no-daemon

FROM openjdk:8-jre-slim

RUN mkdir /app
WORKDIR /app

### TODO: Minor: Templateify and use gradle to insert version number.
COPY --from=build /home/gradle/src/build/libs/twc.repo-crawler-*-all.jar /app/twc.repo-crawler-all.jar

ENTRYPOINT ["java", "-jar", "twc.repo-crawler-all.jar"]