FROM mcr.microsoft.com/dotnet/runtime:5.0 AS base
WORKDIR /app
EXPOSE 88

FROM mcr.microsoft.com/dotnet/sdk:5.0 AS build
WORKDIR /src
COPY ["pgq2/pgq2.csproj", "pgq2/"]
COPY ["pgq2common/pgq2common.csproj", "pgq2common/"]
RUN dotnet restore "pgq2/pgq2.csproj"
COPY . .
WORKDIR "/src/pgq2"
RUN dotnet build "pgq2.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "pgq2.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "pgq2.dll"]