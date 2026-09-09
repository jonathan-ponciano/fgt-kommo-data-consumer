# setup.ps1

# 1. Configura UTF-8
$OutputEncoding = [System.Text.Encoding]::UTF8
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# 2. Variaveis
$repoName = Split-Path -Path $PWD -Leaf
$envName = "env-$repoName"
$pipExe = ".\$envName\Scripts\pip.exe"
$pythonExe = ".\$envName\Scripts\python.exe"
$preCommitExe = ".\$envName\Scripts\pre-commit.exe"
$activateScript = ".\$envName\Scripts\activate.ps1"

Write-Host ""
Write-Host "--- Configurando Ambiente Stalse: $repoName ---" -ForegroundColor Cyan

# 3. Criacao da VENV
if (-not (Test-Path $envName)) {
    Write-Host "1. Criando ambiente virtual (Python 3.11+)..." -ForegroundColor Yellow
    py -m venv $envName
} else {
    Write-Host "1. Ambiente virtual ja identificado." -ForegroundColor Gray
}

# 4. Configuracao do .gitignore
$gitIgnore = ".\.gitignore"
if (-not (Test-Path $gitIgnore)) { New-Item $gitIgnore -ItemType File | Out-Null }
$contentIgnore = Get-Content $gitIgnore -ErrorAction SilentlyContinue

if ($contentIgnore -notcontains "$envName/") {
    Add-Content $gitIgnore "$envName/"
    Write-Host "   -> Venv adicionada ao .gitignore." -ForegroundColor Gray
}
if ($contentIgnore -notcontains ".env") {
    Add-Content $gitIgnore ".env"
    Write-Host "   -> .env adicionado ao .gitignore." -ForegroundColor Gray
}

# 5. Configuracao do .env
if (-not (Test-Path ".env")) {
    Write-Host "2. Criando arquivo .env local..." -ForegroundColor Yellow
    Add-Content ".env" "ENV=development"
    Add-Content ".env" "LOG_LEVEL=DEBUG"
    Add-Content ".env" "# Adicione suas chaves aqui"
}

# 6. Instalacao de Dependencias
if (Test-Path requirements.txt) {
    Write-Host "3. Instalando dependencias..." -ForegroundColor Yellow

    # Atualiza PIP
    & $pythonExe -m pip install --upgrade pip

    # Instala libs
    & $pipExe install -r .\requirements.txt

    # Garante ferramentas de dev
    Write-Host "   -> Garantindo ferramentas de dev..." -ForegroundColor Gray
    & $pipExe install pre-commit ruff mypy pytest commitizen types-requests types-PyYAML
}

# 7. Ativacao dos Hooks
Write-Host "4. Ativando a blindagem do Git (Quality Gates)..." -ForegroundColor Yellow

if (Test-Path $preCommitExe) {
    # Instala os hooks
    & $preCommitExe install --hook-type commit-msg --hook-type pre-commit

    if ($LASTEXITCODE -eq 0) {
        Write-Host "   SUCCESS! Hooks instalados." -ForegroundColor Green

        Write-Host ""
        Write-Host "5. Rodando validacao em todos os arquivos agora..." -ForegroundColor Cyan
        Write-Host "   (Isso cria os ambientes isolados, pode levar alguns minutos na primeira vez)" -ForegroundColor Gray

        # Executa validacao
        & $preCommitExe run --all-files

    } else {
        Write-Host "   ERRO ao configurar hooks." -ForegroundColor Red
    }
} else {
    Write-Host "   ERRO CRITICO: pre-commit nao encontrado." -ForegroundColor Red
}

Write-Host ""
Write-Host "--- Setup Finalizado! ---" -ForegroundColor Cyan
Write-Host "Para continuar no terminal, copie e cole:" -ForegroundColor Gray
Write-Host $activateScript -ForegroundColor White
