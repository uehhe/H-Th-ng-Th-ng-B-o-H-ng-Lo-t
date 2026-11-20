FROM mcr.microsoft.com/dotnet/runtime:10.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY src/Shared/Shared.csproj src/Shared/
COPY src/Notification.Worker/Notification.Worker.csproj src/Notification.Worker/
RUN dotnet restore src/Notification.Worker/Notification.Worker.csproj
COPY . .
RUN dotnet publish src/Notification.Worker/Notification.Worker.csproj -c Release -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "Notification.Worker.dll"]
