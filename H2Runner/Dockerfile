FROM mcr.microsoft.com/dotnet/runtime:5.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:5.0 AS build
WORKDIR /src
COPY ["H2Runner/H2Runner.csproj", "H2Runner/"]
COPY ["pgq2common/pgq2common.csproj", "pgq2common/"]
RUN dotnet restore "H2Runner/H2Runner.csproj"
COPY . .
WORKDIR "/src/H2Runner"
RUN dotnet build "H2Runner.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "H2Runner.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["/bin/sh", "-c", "while true; do echo hello; sleep 10;done"]