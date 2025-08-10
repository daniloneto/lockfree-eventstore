# Multi-stage build for LockFree EventStore sample server (MetricsDashboard)
# Build stage
FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src

# Copy project files & props first (layer caching)
COPY Directory.Build.props ./
COPY src/LockFree.EventStore/LockFree.EventStore.csproj src/LockFree.EventStore/
COPY samples/MetricsDashboard/MetricsDashboard.csproj samples/MetricsDashboard/

# Restore only the web sample (will pull library as ProjectReference)
RUN dotnet restore samples/MetricsDashboard/MetricsDashboard.csproj

# Copy the remaining source
COPY . .

# Publish (framework-dependent)
RUN dotnet publish samples/MetricsDashboard/MetricsDashboard.csproj -c Release -o /app/publish \
    -p:PublishTrimmed=false \
    -p:UseAppHost=false

# Runtime image
FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS final
WORKDIR /app

# Set ASP.NET Core to listen on port 7070 (as per README)
ENV ASPNETCORE_URLS=http://0.0.0.0:7070
EXPOSE 7070

# Copy published output
COPY --from=build /app/publish .

# (Optional) Enable globalization invariant mode for smaller images
# ENV DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1

# Run the web app
ENTRYPOINT ["dotnet", "MetricsDashboard.dll"]

# ---- Notas ----
# 1. Build: docker build -t daniloneto/lockfree-eventstore:latest .
# 2. Run:   docker run --rm -p 7070:7070 daniloneto/lockfree-eventstore:latest
# 3. Native AOT (experimental): add -p:PublishAot=true -r linux-x64 --self-contained true
