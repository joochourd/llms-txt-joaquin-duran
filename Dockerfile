# syntax=docker/dockerfile:1
FROM node:22.21.1-bookworm-slim AS frontend-build
WORKDIR /workspace/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY frontend/ ./
RUN npm test && npm run build

FROM eclipse-temurin:25.0.3_9-jdk-jammy AS backend-build
WORKDIR /workspace
ENV GRADLE_USER_HOME=/workspace/.gradle-home
COPY gradlew gradlew.bat build.gradle.kts settings.gradle.kts gradle.properties .editorconfig ./
COPY gradle/ gradle/
COPY src/ src/
COPY --from=frontend-build /workspace/frontend/dist/ frontend/dist/
RUN chmod +x gradlew && ./gradlew --no-daemon checkKotlinStyle test installDist

FROM eclipse-temurin:25.0.3_9-jre-jammy AS runtime
RUN groupadd --gid 10001 app && useradd --uid 10001 --gid app --no-create-home app
WORKDIR /opt/app
COPY --from=backend-build --chown=app:app /workspace/build/install/llms-txt-joaquin-duran/ ./
USER app
ENV PORT=8080 JAVA_OPTS="-Xmx384m -Dfile.encoding=UTF-8"
EXPOSE 8080
ENTRYPOINT ["/opt/app/bin/llms-txt-joaquin-duran"]
