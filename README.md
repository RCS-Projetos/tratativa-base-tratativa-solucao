# Tratamento Solução (RPA SSW)

Projeto de automação (RPA) desenvolvido em Python para execução de tratativas e rotinas no sistema SSW. A automação utiliza Selenium para navegação web e manipulação do sistema, integrando também resolução de CAPTCHAs via CapSolver.

## 🚀 Tecnologias Utilizadas

- **Linguagem Principal:** Python
- **Automação Web:** Selenium & WebDriver Manager
- **Resolução de CAPTCHA:** CapSolver
- **Manipulação de Dados:** Pandas, FastParquet, PyArrow, OpenPyXL
- **Gerenciador de Pacotes:** `uv`
- **Empacotamento:** PyInstaller

## 📁 Estrutura do Projeto

- `main.py` - Ponto de entrada da aplicação.
- `Automacao_SSW.py` - Classe principal para interação com o sistema SSW.
- `CapSolver.py` - Módulo para resolução de CAPTCHAs.
- `funcoes.py` e `relatorios.py` - Módulos de funções auxiliares e geração de relatórios.
- `_150.py`, `_455.py`, `_930.py` - Módulos com lógicas específicas para cada respectiva rotina ou tela do SSW.
- `pyproject.toml` e `uv.lock` - Configurações do projeto e dependências estritas.
- `RPA Solução.spec` - Especificação de build do PyInstaller.

## ⚙️ Pré-requisitos e Configuração

1. **Python:** Recomendado Python >= 3.14 (baseado em `pyproject.toml`).
2. **Gerenciador de Pacotes:** Utiliza-se a ferramenta de linha de comando `uv` para instalação rápida de dependências.
3. **Variáveis de Ambiente:** É obrigatório possuir um arquivo `.env` na raiz do projeto contendo as credenciais, chaves de API (ex: CapSolver) e dados de autenticação necessários.

## 🛠️ Instalação das Dependências

Crie seu ambiente virtual `.venv` e baixe os pacotes utilizando o `uv`:

```bash
uv sync
```

Caso não possua o `uv` e deseje instalar via `pip`:

```bash
pip install .
```

## ▶️ Uso

Para executar o robô localmente pelo arquivo-fonte:

```bash
python main.py
```

Os arquivos processados (planilhas, .csv, .parquet, bases de dados geradas) são carregados e exportados localmente pelo robô na pasta raiz, conforme lógicas dos módulos, sendo ignorados no controle de versão (`.gitignore`) para fins de segurança e organização.

## 📦 Gerando o Executável

O projeto está configurado para ser empacotado pelo **PyInstaller**, facilitando a distribuição sem a necessidade de instalação do interpretador Python ou das dependências nas máquinas-destino.

Para realizar a compilação do executável `.exe`, rode o comando:

```bash
pyinstaller "RPA Solução.spec"
```
A pasta final gerada e o executável contido nela estarão dentro do diretório `dist/`.
