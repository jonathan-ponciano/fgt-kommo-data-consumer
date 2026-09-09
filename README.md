# 🚀 Stalse Cloud Run Job Template

Este é o template oficial para o desenvolvimento de Jobs de processamento de dados (ETL/ELT) e microsserviços Python na **Stalse**.

Projetado para garantir **Excelência Técnica**, este repositório já vem configurado com:
* **Python 3.12+** e gestão moderna de dependências.
* **Automação de Ambiente** (`setup.ps1`) para onboarding em segundos.
* **Quality Gates** (Pre-commit, Ruff, Mypy) para garantir código limpo e seguro.
* **Infraestrutura como Código** para deploy no Google Cloud Run.

---

## 📋 Stack Tecnológico

| Componente | Tecnologia | Função |
| :--- | :--- | :--- |
| **Linguagem** | Python 3.12 | Runtime principal |
| **Framework** | Pandas / FastAPI | Processamento de dados e API |
| **Qualidade** | Ruff, Mypy | Linting, Formatação e Tipagem Estática |
| **Segurança** | Gitleaks | Prevenção de vazamento de credenciais |
| **Padronização** | Commitizen | Padronização de mensagens de commit |
| **Infra** | Docker, GCP Cloud Run | Containerização e Execução Serverless |

---

## 🛠️ Configuração Inicial (Onboarding)

Siga estes passos para preparar seu ambiente de desenvolvimento local.

### 1. Pré-requisitos
* Python 3.11+ instalado.
* Git instalado.
* Google Cloud SDK (gcloud) instalado e autenticado.

### 2. Setup Automatizado ⚡
Execute o script de configuração na raiz do projeto. Ele criará a `venv`, instalará as dependências e ativará os guardiões de qualidade (hooks do git).

```powershell
.\setup.ps1
```

> **O que este script faz?**
> 1. Cria um ambiente virtual isolado (`env-<nome-do-repo>`).
> 2. Gera um arquivo `.env` local para suas variáveis de ambiente.
> 3. Instala todas as bibliotecas do `requirements.txt`.
> 4. **Instala e ativa o Pre-commit** para proteger seus commits.

### 3. Ativar o Ambiente

Caso o script não ative automaticamente, execute:

```powershell
.\env-sua-env\Scripts\activate
```

---

## 🛡️ Padrões de Engenharia (Quality Gates)

Na Stalse, a qualidade não é negociável. Utilizamos ferramentas automáticas para garantir o padrão.

### Regras de Ouro

1. **Tipagem:** Embora opcional no início, encorajamos o uso de tipagem forte (`def func() -> str:`).
2. **Documentação:** Todas as funções devem ter **Docstrings** no padrão Google.
```python
def process_data(df):
    """Processa o dataframe de entrada.

    Args:
        df: Dataframe bruto.
    """
    ...

```


3. **Commits:** Mensagens fora do padrão **serão bloqueadas**. Use [Conventional Commits](https://www.conventionalcommits.org/):
* `feat: Adiciona conexão com BigQuery`
* `fix: Corrige erro de cálculo de ROI`
* `docs: Atualiza instruções de deploy`
* `refactor: Melhora performance do ETL`



### 🧰 Comandos Úteis (Dia a Dia)

**1. Validar tudo (Antes de commitar)**
Roda todos os checks (Linter, Tipos, Segurança) em todos os arquivos.

```bash
pre-commit run --all-files
```

**2. Ruff (Linter & Formatador Rápido)**
O Ruff substitui o Flake8, Black e Isort. Use para manter o código limpo.

```bash
# Apenas verificar erros (não altera arquivos)
ruff check .

# ✨ MÁGICA: Corrigir erros automaticamente (imports, variáveis não usadas, etc)
ruff check --fix .

# Formatar código (ajustar identação, aspas e espaços)
ruff format .

```

**3. Testes**
Executa a bateria de testes unitários.

```bash
pytest
```

---

## 🚀 Desenvolvimento e Execução

### Rodar Localmente

Para testar a lógica do Job sem subir para a nuvem:

```bash
python main.py
```

### Estrutura de Arquivos

```plaintext
.
├── main.py                 # Lógica Principal (ETL: Extract, Transform, Load)
├── deploy.ps1              # Script de Deploy para Google Cloud
├── setup.ps1               # Script de Configuração de Ambiente
├── Dockerfile              # Definição da Imagem do Container
├── requirements.txt        # Dependências de Produção e Dev
├── pyproject.toml          # Configuração das Ferramentas de Qualidade
├── .pre-commit-config.yaml # Configuração dos Hooks do Git
└── test/                   # Testes Unitários

```

---

## ☁️ Deploy (Google Cloud Run)

O deploy é automatizado via script, garantindo que suas configurações locais (seguras) sejam replicadas no ambiente de nuvem.

### Passo a Passo

1. Garanta que seu `.env` tem as variáveis necessárias.
2. Execute o script de deploy:
```powershell
.\deploy.ps1
```


3. O assistente solicitará:
* **Project ID** do GCP.
* **Região** (padrão: `us-central1`).
* **Nome do Job**.
* **Agendamento** (Cron) para o Cloud Scheduler.



> **Nota:** O script constrói a imagem Docker, sobe para o Container Registry e atualiza o Job no Cloud Run, injetando as variáveis de ambiente automaticamente.

---

## 🤝 Contribuição

1. Crie uma branch para sua tarefa: `git checkout -b feat/nova-feature`
2. Desenvolva e garanta que os testes passem (`pytest`).
3. Commite suas alterações (o `pre-commit` irá validar e formatar seu código).
4. Abra um Pull Request para a branch `main`.
