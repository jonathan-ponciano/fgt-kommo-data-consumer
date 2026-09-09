# deploy.ps1

# --- CORRECAO DE ACENTUACAO (UTF-8) ---
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
# --------------------------------------

# 1. Configuracoes Iniciais
$repoName = Split-Path -Path $PWD -Leaf
Write-Host "--- Deploy Automatico + Scheduler: $repoName ---" -ForegroundColor Cyan

# Verifica se o Gcloud esta instalado
if (-not (Get-Command "gcloud" -ErrorAction SilentlyContinue)) {
    Write-Host "Erro: Google Cloud SDK (gcloud) nao encontrado no PATH." -ForegroundColor Red
    exit 1
}

# 2. Coleta de Informacoes do Usuario
$projectId = Read-Host "Digite o Google Cloud Project ID"
if ([string]::IsNullOrWhiteSpace($projectId)) {
    Write-Host "Erro: O Project ID e obrigatorio." -ForegroundColor Red
    exit 1
}

$region = Read-Host "Digite a Regiao (padrao: us-central1)"
if ([string]::IsNullOrWhiteSpace($region)) { $region = "us-central1" }

$jobName = Read-Host "Nome do Job (padrao: $repoName)"
if ([string]::IsNullOrWhiteSpace($jobName)) { $jobName = $repoName }

# Define a tag da imagem
$imageTag = "gcr.io/$projectId/$jobName"

# ---------------------------------------------------------
# ETAPA PREVIA: LER VARIAVEIS DO .ENV
# ---------------------------------------------------------
$envVarsFlag = ""
if (Test-Path ".env") {
    Write-Host "`nDetectado arquivo .env. Lendo variaveis..." -ForegroundColor Cyan
    $envContent = Get-Content ".env"
    $envList = @()

    foreach ($line in $envContent) {
        $line = $line.Trim()
        # Ignora linhas vazias ou comentarios (#)
        if (-not [string]::IsNullOrWhiteSpace($line) -and -not $line.StartsWith("#")) {
            $envList += $line
        }
    }

    if ($envList.Count -gt 0) {
        # Junta as variaveis com virgula: VAR1=val1,VAR2=val2
        $envString = $envList -join ","
        $envVarsFlag = "--set-env-vars `"$envString`""
        Write-Host "   -> Variaveis preparadas para envio: $envList" -ForegroundColor Gray
    } else {
        Write-Host "   -> Arquivo .env vazio ou apenas com comentarios." -ForegroundColor Gray
    }
}

# ---------------------------------------------------------
# ETAPA 1: BUILD
# ---------------------------------------------------------
Write-Host "`n1. Iniciando Build da Imagem Docker..." -ForegroundColor Yellow
Write-Host "   Target: $imageTag" -ForegroundColor Gray

cmd /c "gcloud builds submit --tag $imageTag ."
if ($LASTEXITCODE -ne 0) {
    Write-Host "Erro no Build. Abortando deploy." -ForegroundColor Red
    exit $LASTEXITCODE
}

# ---------------------------------------------------------
# ETAPA 2: DEPLOY DO CLOUD RUN JOB
# ---------------------------------------------------------
Write-Host "`n2. Verificando Cloud Run Job..." -ForegroundColor Yellow

$jobExists = $false
try {
    # Verifica existencia do job
    cmd /c "gcloud run jobs describe $jobName --region $region --format=value(metadata.name) 2>&1" | Out-Null
    if ($LASTEXITCODE -eq 0) { $jobExists = $true }
} catch { $jobExists = $false }

if ($jobExists) {
    Write-Host "   Job '$jobName' encontrado. Atualizando..." -ForegroundColor Cyan
    # Adiciona a flag de env vars se existir
    $cmdUpdate = "gcloud run jobs update $jobName --image $imageTag --region $region $envVarsFlag"
    Invoke-Expression $cmdUpdate
} else {
    Write-Host "   Job '$jobName' nao encontrado. Criando novo..." -ForegroundColor Green
    # Adiciona a flag de env vars se existir
    $cmdCreate = "gcloud run jobs create $jobName --image $imageTag --region $region --tasks 1 --max-retries 3 --memory 512Mi $envVarsFlag"
    Invoke-Expression $cmdCreate
}

if ($LASTEXITCODE -ne 0) {
    Write-Host "Erro no Deploy do Job. Abortando." -ForegroundColor Red
    exit $LASTEXITCODE
}

# ---------------------------------------------------------
# ETAPA 3: CONFIGURACAO DO CLOUD SCHEDULER (HTTP METHOD)
# ---------------------------------------------------------
Write-Host "`n3. Configuracao de Agendamento (Cloud Scheduler)..." -ForegroundColor Yellow
$schedResponse = Read-Host "Deseja agendar/atualizar o cron deste Job? (S/N)"

if ($schedResponse -eq 'S' -or $schedResponse -eq 's') {

    # --- LOGICA DE CRON PADRAO ---
    Write-Host "   Pressione ENTER para usar o padrao (03:00 am todo dia)." -ForegroundColor Gray
    $inputCron = Read-Host "Digite o Cron Schedule"

    if ([string]::IsNullOrWhiteSpace($inputCron)) {
        $cronSchedule = "0 3 * * *"
        Write-Host "   -> Usando Cron padrao: $cronSchedule" -ForegroundColor Green
    } else {
        $cronSchedule = $inputCron
        Write-Host "   -> Usando Cron definido: $cronSchedule" -ForegroundColor Green
    }

    # --- LOGICA DE TIMEZONE ---
    $timeZone = "America/Sao_Paulo"

    $schedulerName = "scheduler-$jobName"

    # 3.1 Busca o Project Number para definir o Service Account padrao
    Write-Host "   Obtendo informacoes do projeto..." -ForegroundColor Gray

    $rawProjectNumber = cmd /c "gcloud projects describe $projectId --format=value(projectNumber)"
    $projectNumber = $rawProjectNumber.Trim()

    if (-not [string]::IsNullOrWhiteSpace($projectNumber)) {
        $saEmail = "$projectNumber-compute@developer.gserviceaccount.com"
        Write-Host "   Usando Service Account padrao: $saEmail" -ForegroundColor Gray

        # 3.2 Define a URL da API do Cloud Run (v2) para disparo
        $jobUri = "https://run.googleapis.com/v2/projects/$projectId/locations/$region/jobs/$jobName`:run"

        # 3.3 Verifica se o agendador ja existe
        $schedulerExists = $false
        try {
            cmd /c "gcloud scheduler jobs describe $schedulerName --location $region 2>&1" | Out-Null
            if ($LASTEXITCODE -eq 0) { $schedulerExists = $true }
        } catch { $schedulerExists = $false }

        if ($schedulerExists) {
            Write-Host "   Atualizando agendador '$schedulerName'..." -ForegroundColor Cyan
            cmd /c "gcloud scheduler jobs update http $schedulerName --schedule `"$cronSchedule`" --time-zone `"$timeZone`" --uri `"$jobUri`" --http-method POST --oidc-service-account-email `"$saEmail`" --location $region"
        } else {
            Write-Host "   Criando novo agendador '$schedulerName'..." -ForegroundColor Green
            cmd /c "gcloud scheduler jobs create http $schedulerName --schedule `"$cronSchedule`" --time-zone `"$timeZone`" --uri `"$jobUri`" --http-method POST --oidc-service-account-email `"$saEmail`" --location $region"
        }
    } else {
        Write-Host "   Erro: Nao foi possivel obter o Project Number. Verifique seu Project ID." -ForegroundColor Red
    }
}

Write-Host "`n--- Processo Finalizado! ---" -ForegroundColor Green
