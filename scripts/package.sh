set -ex

cd $(dirname $0)/../

artifactsFolder="./artifacts"

if [ -d $artifactsFolder ]; then
  rm -R $artifactsFolder
fi

mkdir -p $artifactsFolder

dotnet restore ./RedisMessageBus.sln
dotnet build ./RedisMessageBus.sln -c Release


dotnet pack ./src/Aix.RedisMessageBus/Aix.RedisMessageBus.csproj -c Release -o $artifactsFolder
