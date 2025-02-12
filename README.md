# 📊 Build da  Aplicação

Siga os passos abaixo:

- Instale o Docker e Docker-Compose

- git clone https://github.com/juliocms/airflow-postgress-spark-python.git

- cd airflow-postgress-spark-python

- Por nao conseguir baixar via apt-get na image do docker, tive que colocar o jdk11 manualmente. Isso devido as dependencias do teste e airflow. Por isso, crie uma pasta chamada lib e baixo o pacote openlogic-openjdk-11.0.26+4-linux-x64.tar.gz para esta pasta. Busque no endereco https://www.openlogic.com/openjdk-downloads?field_java_parent_version_target_id=406&field_operating_system_target_id=426&field_architecture_target_id=391&field_java_package_target_id=396 

- docker-compose up -d --build

- se quiser verificar se os testes passaram digite "docker logs python -f". No final é exibida a porcentagem de testes que passaram.

- O Airflow é o último a subir, pode verificar se quiser com o comando "docker logs airflow -f".

- Abra o Browser e digite http://localhost:8081/login. Usuario e senha "admin" 

- Startar o Airflow

- Após a execução pare o docker compose com "docker-compose down -v"

---

# 📊 Monitoramento e Alertas para o Pipeline de Dados

## 📌 1. Monitoramento da Execução do Pipeline

O Airflow possui ferramentas nativas de monitoramento, mas podemos aprimorar isso com algumas estratégias adicionais:

### ✅ Monitoramento de DAGs e Tarefas
- Capturar exceções e falhas em tarefas para gerar alertas automáticos em ferramentas como Datadog, Grafana, entre outros
- Criar um DAG específico para monitoramento de outros DAGs, checando o sucesso das mesmas

### 📜 Logs Centralizados
- Configurar logs para serem armazenados em **Azure App Insights**, **Grafana Loki** ou **AWS CloudWatch**.
- Criar alertas baseados em padrões de falha nos logs nessas ferramentas.

### 📈 Métricas e Health Checks
- Criar dashboards no **Grafana** para visualizar desempenho do pipeline.

---

## 🛠 2. Monitoramento da Qualidade dos Dados

Para evitar problemas com dados inconsistentes ou corrompidos, podemos implementar verificações automáticas:

### 🔍 Data Quality Checks no Airflow
- Podemos utilizar o **Great Expectations** para validar qualidade dos dados.
- Criar operadores customizados para checar padrões específicos nos datasets.

### 🧪 Testes Automáticos no Pipeline
- Criar testes na pasta de testes do ambiente Docker para validar integridade referencial e valores nulos por exemplo.
- Usar **dbt (Data Build Tool)** para validar integridade referencial e valores nulos.

---

## 🚨 3. Sistema de Alertas e Notificações

Quando falhas ou anomalias forem detectadas, os alertas devem ser enviados automaticamente. Algumas abordagens incluem:

### 📩 Notificações no Airflow
- Configurar **e-mails automáticos** via `on_failure_callback` e `on_success_callback`.
- Integrar com ferramentas como **Slack, Microsoft Teams ou Telegram** via Webhooks.

### 🔄 Retries e Auto-recovery
- Configurar **retries automáticos** (`retries=3` com `retry_delay=timedelta(minutes=5)`).
- Implementar lógica de **fallback** para consumo de datasets antigos em caso de falha.

