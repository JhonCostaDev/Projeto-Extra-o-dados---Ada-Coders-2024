# Projeto de Extração de Dados a partir das API's do IPEA e IBGE

## Objetivo
Realização de um ETL - Extract, Transform, Load. que consiste em:
  * Extract: Reunir os dados das API's.
  * Transform: Fazer as conversões e limpeza.
  * Load: Salvar os Dados consolidados em um banco de dados, ou warehouse para que possam ser consultados e analisados.

### Alvo:
Catalogar a série histórica de feminicídios no Brasil.

Fontes: 
* [IPEA - Instituto de Pesquisa Econômica Aplicada](http://www.ipeadata.gov.br)
* [IBGE - Instituto Brasileiro de Geografia e Estatística](https://www.ibge.gov.br/)

## Sobre as API's
### IPEA
O site do IPEA disponibiliza uma biblioteca Python que ajuda nas requisições da API, e possui um repositório no [GitHub](https://github.com/luanborelli/ipeadatapy/) com sua documentação. É através dela que extraímos os dados sobre feminicídio.

### IBGE
O IBGE também disponibiliza uma [API](https://servicodados.ibge.gov.br/api/docs/localidades) para consultas de código de região onde utilizamos para melhor idêntificação dos territórios, já que a API do IPEA identifica apenas por código de território. 

### Projeto no Databriks
[Clique aqui para acessar o notebook no Databriks]([https://community.cloud.databricks.com/?o=2529677648243962#notebook/2033834618209058](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/2529677648243962/2033834618209058/5227400064554836/latest.html)
