FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /source
COPY pom.xml .
COPY src src
COPY render-maven-settings.xml .
RUN mvn -B -s render-maven-settings.xml clean package
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /source/target/runbook-agent-1.0.0.jar app.jar
USER 10001
ENTRYPOINT ["java","-XX:MaxRAMPercentage=65","-jar","app.jar"]
