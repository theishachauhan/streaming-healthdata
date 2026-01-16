# Use Eclipse Temurin (Official Java 17 Image)
FROM eclipse-temurin:17-jre

WORKDIR /app

# Download the latest stable version of Synthea
ADD https://github.com/synthetichealth/synthea/releases/download/master-branch-latest/synthea-with-dependencies.jar synthea.jar

# Run it
ENTRYPOINT ["java", "-jar", "synthea.jar"]