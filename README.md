![Pipeline ETL](https://raw.githubusercontent.com/devrafael26/Pipeline-ETL-Data-Analytics-AWS/main/Pipeline%20ETL%20Olist.png)

Pipeline ETL para Análise de Vendas da Olist, desenvolvido na AWS.
Os dados utilizados foram extraídos do Kaggle.
Como funciona?
1. Ingestão de Dados: Os datasets foram carregados para o Amazon S3.
   
2. Processamento e Transformação: Utilizei o AWS Glue com PySpark para tratar e transformar os dados.
   
3. Armazenamento e Consulta: Após a transformação, os dados foram armazenados no Amazon Redshift, onde criei as tabelas do Data Warehouse e realizei análises com SQL.
   
4. Visualização: As análises foram enviadas para o Amazon QuickSight, onde os gráficos e dashboards foram criados.
