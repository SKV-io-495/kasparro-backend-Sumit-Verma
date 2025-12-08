param (
    [string]$Command
)

switch ($Command) {
    "up" {
        docker-compose up --build -d
    }
    "down" {
        docker-compose down
    }
    "test" {
        docker-compose run --rm backend pytest -p no:cacheprovider -s
    }
    Default {
        Write-Host "Usage: .\manage.ps1 [up|down|test]"
        Write-Host "  up   : Start services"
        Write-Host "  down : Stop services"
        Write-Host "  test : Run tests"
    }
}
