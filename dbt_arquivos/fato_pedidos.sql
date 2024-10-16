{{ config(materialized="table", alias="fato_pedidos") }}

with
    fato as (
        select * from {{ ref("tab_pedidos") }}
    ),
    fato_pedidos as (
        select 
            CAST("Lucro" AS DECIMAL(10, 2)) AS "Lucro",
            CAST("Pais" AS VARCHAR) AS "Pais",
            CAST("Cidade" AS VARCHAR) AS "Cidade",
            CAST("Estado" AS VARCHAR) AS "Estado",
            CAST("Vendas" AS DECIMAL(10, 2)) AS "Vendas",
            CAST("Regiao" AS VARCHAR) AS "Regiao",
            CAST("Desconto" AS DECIMAL(10, 2)) AS "Desconto",
            CAST("ID_Ordem" AS INTEGER) AS "ID_Ordem",
            CAST("Segmento" AS VARCHAR) AS "Segmento",
            CAST("CD_Postal" AS VARCHAR) AS "CD_Postal",
            CAST("Categoria" AS VARCHAR) AS "Categoria",
            CAST("ID_Pedido" AS VARCHAR) AS "ID_Pedido",
            CAST("Data_Envio" AS DATE) AS "Data_Envio",
            CAST("ID_Cliente" AS VARCHAR) AS "ID_Cliente",
            CAST("ID_Produto" AS VARCHAR) AS "ID_Produto",
            CAST("Quantidade" AS INTEGER) AS "Quantidade",
            CAST("Tipo_Envio" AS VARCHAR) AS "Tipo_Envio",
            -- Certifique-se de que Data_Pedido está no formato DATE
            CAST("Data_Pedido" AS DATE) AS "Data_Pedido",
            CAST("Nome_Cliente" AS VARCHAR) AS "Nome_Cliente",
            CAST("Nome_Produto" AS VARCHAR) AS "Nome_Produto",
            CAST("Sub_Categoria" AS VARCHAR) AS "Sub_Categoria",
            (CAST("Data_Envio" AS DATE) - CAST("Data_Pedido" AS DATE)) AS "Tempo_envio",

            -- Adiciona o número do mês
            CAST(EXTRACT(MONTH FROM CAST("Data_Pedido" AS DATE)) AS INTEGER) AS mes_num,
            -- Adiciona o nome do mês
            CASE 
                WHEN EXTRACT(MONTH FROM CAST("Data_Pedido" AS DATE)) = 1 THEN 'Jan'
                WHEN EXTRACT(MONTH FROM CAST("Data_Pedido" AS DATE)) = 2 THEN 'Fev'
                WHEN EXTRACT(MONTH FROM CAST("Data_Pedido" AS DATE)) = 3 THEN 'Mar'
                WHEN EXTRACT(MONTH FROM CAST("Data_Pedido" AS DATE)) = 4 THEN 'Abr'
                WHEN EXTRACT(MONTH FROM CAST("Data_Pedido" AS DATE)) = 5 THEN 'Mai'
                WHEN EXTRACT(MONTH FROM CAST("Data_Pedido" AS DATE)) = 6 THEN 'Jun'
                WHEN EXTRACT(MONTH FROM CAST("Data_Pedido" AS DATE)) = 7 THEN 'Jul'
                WHEN EXTRACT(MONTH FROM CAST("Data_Pedido" AS DATE)) = 8 THEN 'Ago'
                WHEN EXTRACT(MONTH FROM CAST("Data_Pedido" AS DATE)) = 9 THEN 'Set'
                WHEN EXTRACT(MONTH FROM CAST("Data_Pedido" AS DATE)) = 10 THEN 'Out'
                WHEN EXTRACT(MONTH FROM CAST("Data_Pedido" AS DATE)) = 11 THEN 'Nov'
                WHEN EXTRACT(MONTH FROM CAST("Data_Pedido" AS DATE)) = 12 THEN 'Dez'
            END AS mes_nome
            
        from fato
    )
select *
from fato_pedidos
