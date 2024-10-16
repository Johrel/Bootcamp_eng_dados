with
    tab_pedidos as (
        select "Lucro", "Pais", "Cidade", "Estado", "Vendas", "Regiao", "Desconto", 
        "ID_Ordem", "Segmento", "CD_Postal", "Categoria", "ID_Pedido", "Data_Envio", 
        "ID_Cliente", "ID_Produto", "Quantidade", "Tipo_Envio", "Data_Pedido", "Nome_Cliente", 
        "Nome_Produto", "Sub_Categoria"
        from {{ source("bronze", "google_sheets_Sheet1") }}
    )

select * from tab_pedidos