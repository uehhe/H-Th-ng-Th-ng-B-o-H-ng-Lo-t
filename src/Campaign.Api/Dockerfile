FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS base
WORKDIR /app
EXPOSE 8080

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY src/Shared/Shared.csproj src/Shared/
COPY src/Campaign.Api/Campaign.Api.csproj src/Campaign.Api/
RUN dotnet restore src/Campaign.Api/Campaign.Api.csproj
COPY . .
RUN dotnet publish src/Campaign.Api/Campaign.Api.csproj -c Release -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "Campaign.Api.dll"]
