#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/aspnet:5.0 AS base
WORKDIR /app
EXPOSE 80
ENV ASPNETCORE_ENVIRONMENT=Development

FROM mcr.microsoft.com/dotnet/sdk:5.0 AS build
WORKDIR /src
COPY ["pgq/pgq.csproj", "pgq/"]
RUN dotnet restore "pgq/pgq.csproj"
COPY . .
WORKDIR "/src/pgq"
RUN dotnet build "pgq.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "pgq.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "pgq.dll"]