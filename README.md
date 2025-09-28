# Importador de de tabelas 
<p> Realiza a importação de um range de tabelas de um banco de dados X para banco Y necessitando apenas da informação do banco origem e o range de tabela. </p>
<img width="770" height="550" alt="image" src="https://github.com/user-attachments/assets/162bf8ff-08c7-4490-a83b-e0e640fb2b35" />
<br>
<p> O fonte realiza uma query para verificar somente as tabelas que possuem dados no outro banco e tabelas criadas pelo sistema ou usuario (Não inclui tabelas de dicionario ou tabelas internas como as tabelas SYS).</p>
<p>O mesmo também realiza um truncate na tale destino para incluir os dados.</p>
