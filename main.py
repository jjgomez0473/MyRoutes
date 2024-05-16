import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from io import BytesIO

UPLOAD_FOLDER = st.secrets["UPLOAD_FOLDER"]

current_year = datetime.now().year
day_process = datetime.now() + timedelta(days=1)
name = "JM&cm"

def data_claims():
    list_attributes = ["Timestamp", "Order ID", "Drawback", "Problem description", "Responsable"]
    url = "https://docs.google.com/spreadsheets/d/1DBTlOmvFvsxACJuYvdS2fnDXKcAYxXji2hrvs6yIwF8/edit?usp=sharing"
    # Extraer el ID del documento
    documento_id = url.split("/")[5]
    # URL de exportación CSV
    csv_url = f"https://docs.google.com/spreadsheets/d/{documento_id}/export?format=csv"
    # Cargar el conjunto de datos en un DataFrame de pandas
    df = pd.read_csv(csv_url)
    return df[list_attributes]


# Carga todos los datos para el proceso en datasets
def load_data(file_csv):
    # archivo_arg = r"datos_pedidos.csv"
    auxiliar = r"aux_ruteo.xlsx"
    df_pedidos_ar = pd.read_csv( file_csv, encoding="latin-1")    
    df_aux_pup = pd.read_excel(UPLOAD_FOLDER + auxiliar, sheet_name="pup").query('Activo == 1')
    df_aux_status = pd.read_excel(UPLOAD_FOLDER + auxiliar, sheet_name="status").query('si_no == "Sí"')
    df_aux_id = pd.read_excel(UPLOAD_FOLDER + auxiliar, sheet_name="product_id")
    df_aux_picking = pd.read_excel(UPLOAD_FOLDER + auxiliar, sheet_name="picking")
    df_aux_zone = pd.read_excel(UPLOAD_FOLDER + auxiliar, sheet_name="zonas")
    df_aux_hruta = pd.read_excel(UPLOAD_FOLDER + auxiliar, sheet_name="hoja_ruta")
    return df_pedidos_ar, df_aux_pup, df_aux_status, df_aux_id, df_aux_picking, df_aux_zone, df_aux_hruta

# Generar listas y diccionarios para aplicar a filtros.
def filter_data(df_aux_status, df_aux_id, df_aux_pup, df_aux_picking, df_aux_zone):
    list_status = df_aux_status["order_status"].to_list()
    list_id = df_aux_id["id"].to_list()
    list_columns_aux_pup = df_aux_pup.columns.to_list()[1:]
    list_picking = df_aux_picking["sku"].to_list()
    dict_zones = df_aux_zone[["nombre", "min_prom_parada"]].to_dict()
    return list_status, list_id, list_columns_aux_pup, list_picking, dict_zones

# Prepara los datos para proceso y análisis
def prepare_data(df_pedidos_ar, df_aux_pup):
    #convertir columnas  de fecha
   
    df_pedidos_ar["Fecha_de_entrega_solicitada"] = pd.to_datetime(df_pedidos_ar["Fecha_de_entrega_solicitada"], format="%Y-%m-%d")
    df_pedidos_ar["Fecha_PuP"] = pd.to_datetime(df_pedidos_ar["Fecha_PuP"], format="%d-%m-%Y")
    # Reemplazar los valores de 'Fecha_de_entrega_solicitada' por los de 'Fecha_PuP' donde el Metodo_de_envio sea  "RETIRO EN PICKUP POINT"
    df_pedidos_ar.loc[df_pedidos_ar["Metodo_de_envio"] == "RETIRO EN PICKUP POINT", "Fecha_de_entrega_solicitada"] = df_pedidos_ar.loc[df_pedidos_ar["Metodo_de_envio"] == "RETIRO EN PICKUP POINT", "Fecha_PuP"]
    df_pedidos_ar.loc[df_pedidos_ar["Metodo_de_envio"] == "RETIRO EN PICKUP POINT", "Dir_Verificada"] = "Verificada"
    # Eliminar la columna 'fecha_entrega_maxima' si ya no la necesitas
    df_pedidos_ar.drop(columns=["Fecha_PuP"], inplace=True)
    # Agregar nuevas columnas
    # Concatenar nombre
    df_pedidos_ar["fullname"] = df_pedidos_ar["firstname"] + " " + df_pedidos_ar["lastname"]
    #Entrega o retiro
    df_pedidos_ar["delivery_recall"] = "E"
    # Diccionario condatos auxiliares 
    df_pedidos_ar["Dir_Verificada"] = df_pedidos_ar["Dir_Verificada"].fillna("No detectada")  
    df_pedidos_ar["Pedido_comentarios"] = df_pedidos_ar["Pedido_comentarios"].fillna("Sin comentarios")  
    df_pedidos_ar["data_custom"] = df_pedidos_ar["Dir_Verificada"] + "," + df_pedidos_ar["Pedido_comentarios"]
    df_pedidos_ar= df_pedidos_ar.drop(["Pedido_comentarios","Dir_Verificada"], axis=1)    
    # Reemplazar los valores en 'atributo_a' con los valores de 'atributo_b' donde 'pup' coincide
    for index,row in df_aux_pup.iterrows():
      df_pedidos_ar.loc[df_pedidos_ar["Pickup_point"] == row["Pickup_point"], "zona"] = row["zona"]
      df_pedidos_ar.loc[df_pedidos_ar["Pickup_point"] == row["Pickup_point"], "Desde_hora"] = row["Desde_hora"]
      df_pedidos_ar.loc[df_pedidos_ar["Pickup_point"] == row["Pickup_point"], "Hasta_hora"] = row["Hasta_hora"]
      df_pedidos_ar.loc[df_pedidos_ar["Pickup_point"] == row["Pickup_point"], "shipping_street_full"] = row["shipping_street_full"]
      df_pedidos_ar.loc[df_pedidos_ar["Pickup_point"] == row["Pickup_point"], "shipping_postcode"] = row["shipping_postcode"]
      df_pedidos_ar.loc[df_pedidos_ar["Pickup_point"] == row["Pickup_point"], "shipping_city"] = row["shipping_city"]
      df_pedidos_ar.loc[df_pedidos_ar["Pickup_point"] == row["Pickup_point"], "shipping_region"] = row["shipping_region"]
      df_pedidos_ar.loc[df_pedidos_ar["Pickup_point"] == row["Pickup_point"], "shipping_country"] = row["shipping_country"]
      df_pedidos_ar.loc[df_pedidos_ar["Pickup_point"] == row["Pickup_point"], "Latitud"] = row["Latitud"]
      df_pedidos_ar.loc[df_pedidos_ar["Pickup_point"] == row["Pickup_point"], "Longitud"] = row["Longitud"]      
    return df_pedidos_ar

# Aplica los filtros 
def apply_filters(df_pedidos_ar, list_status, list_id):
    #Fitros para depurar registros
    filter_status = df_pedidos_ar["order_status"].isin(list_status)
    filter_id = ~df_pedidos_ar["Producto_pedido_id"].isin(list_id)
    df_filtered = df_pedidos_ar[filter_status & filter_id]
    df_filtered["Aclaraciones"]= df_filtered["Aclaraciones"].fillna("")
    df_filtered["data_custom"]= df_filtered["data_custom"].fillna("")
    df_filtered["shipping_street_comment"]= df_filtered["shipping_street_comment"].fillna("")
    return df_filtered

# Identifica productos que se colocan en contenedor especial
def in_bag(df_filtered, list_picking):
    # Construir el diccionario
    picking_dict = {}
    for index, row in df_filtered.iterrows():
        numero_pedido = row['order_id']
        articulo = row['Producto_pedido_id']
        sku = row['product_sku']
        cantidad = row['qty_ordered']        
        # Verificar si el artículo está en la lista de artículos conocidos
        if articulo in list_picking:
            # Si el número de pedido no está en el diccionario, inicializar una lista vacía para ese número de pedido
            if numero_pedido not in picking_dict:
                picking_dict[numero_pedido] = ""
            # Agregar el artículo y la cantidad al diccionario
            picking_dict[numero_pedido] += f"{sku}({cantidad})-"
            
    for numero_pedido in picking_dict:
        picking_dict[numero_pedido] = picking_dict[numero_pedido][:-1]

    return picking_dict

# Agrupa de acuerdo a formato de ruteo preliminar
def group_data(df_filtered):
    result_grouped = df_filtered.groupby(["Fecha_de_entrega_solicitada", "Pedido_sucursal", "order_id","zona", "fullname", "email", "shipping_street_full", "shipping_postcode", "shipping_city", "shipping_region", "shipping_country", 
                                    "shipping_street_comment", "delivery_recall", "shipping_telephone", "Desde_hora", "Hasta_hora", "Latitud", "Longitud", "data_custom",  
                                    "Aclaraciones"]).agg({"qty_ordered": "sum"})
    df_grouped = result_grouped.reset_index()
    return df_grouped

# Recorrer el DataFrame original y actualizar el atributo 'Pedido_comentarios' usando los datos del diccionario
def update_comment(df_grouped, picking_dict):
    for index, row in df_grouped.iterrows():
        numero_pedido = row["order_id"]
        aclaraciones = row["Aclaraciones"]
        if numero_pedido in picking_dict:
            valor = picking_dict[numero_pedido]        
            if  aclaraciones != "":            
                nueva_aclaraciones = aclaraciones + " || " + valor
            else:
                nueva_aclaraciones = valor            
            df_grouped.at[index, "Aclaraciones"] = nueva_aclaraciones
    
    return df_grouped

# Detecta pedidos unificados comparando lat y lon, calcula nuevos tiempos y actualiza tiempo de parada
def unify_orders(df_grouped, dict_zones):
    df_grouped["lat_lon"] = df_grouped["Latitud"].astype(str) +  df_grouped["Longitud"].astype(str) 
    # Mapear los valores de 'min_prom_parada' según la correspondencia entre las zonas
    df_diccionario = pd.DataFrame(dict_zones)
    df_grouped['min_parada'] = df_grouped['zona'].map(df_diccionario.set_index('nombre')['min_prom_parada'])
    # Crea diccionario de ubicaciones repetidas y coloca las mayores a 1 con suma de cantidad y  de bultos 
    group_location = df_grouped.groupby("lat_lon").agg({"order_id":"count", "qty_ordered": "sum", "min_parada": "mean"}).reset_index()
    group_location = group_location.sort_values(by="order_id", ascending=False)
    group_location["vueltas"]= round(group_location["qty_ordered"] / 10)
    group_location = group_location.loc[group_location["order_id"] > 1]
    group_location.loc[group_location["vueltas"] == 0, "vueltas"] = 1
    group_location["min_parada_prom"]= round(group_location["vueltas"] * group_location["min_parada"] / group_location["order_id"])
    # Reemplazar los valores en 'atributo_a' con los valores de 'atributo_b' donde 'pup' coincide
    for index,row in group_location.iterrows():
      df_grouped.loc[df_grouped["lat_lon"] == row["lat_lon"], "min_parada"] = row["min_parada_prom"] 
      df_grouped.loc[df_grouped["lat_lon"] == row["lat_lon"], "data_custom"] = df_grouped["data_custom"] + ",Unificado"  
      
    return df_grouped 

# Crear hoja de ruta
def create_roadmap(df_grouped, df_aux_hruta):
    # Cambiar los nombres de los atributos en datos_pedidos según la tabla de conversión
    conversion_dict = dict(zip(df_aux_hruta['atrib_admin'],df_aux_hruta['atrib_oc']))
    df_grouped.rename(columns=conversion_dict, inplace=True) 
    # Obtener la lista de atributos necesarios según atrib_oc en aux_ruteo
    atributos_necesarios = df_aux_hruta['atrib_oc'].unique()
    # Asegurar que todos los atributos necesarios están en df_datos_pedidos
    for atributo in atributos_necesarios:
        if atributo not in df_grouped.columns:
            df_grouped[atributo] = ""    
    #  Seleccionar solo los atributos necesarios y descartar el resto
    df_grouped = df_grouped[atributos_necesarios] 
    return df_grouped 


def model_process(file_csv, value_date):
    df_pedidos_ar, df_aux_pup, df_aux_status, df_aux_id, df_aux_picking, df_aux_zone, df_aux_hruta = load_data(file_csv)
    list_status, list_id, list_columns_aux_pup, list_picking, dict_zones = filter_data(df_aux_status, df_aux_id, df_aux_pup, df_aux_picking, df_aux_zone)
    df_pedidos_ar = prepare_data(df_pedidos_ar, df_aux_pup)
    
    # Filtrar fecha a procesar

    filter_date = df_pedidos_ar["Fecha_de_entrega_solicitada"] == str(value_date)
    df_pedidos_ar = df_pedidos_ar[filter_date]    
    
    df_filtered = apply_filters(df_pedidos_ar, list_status, list_id)
    picking_dict = in_bag(df_filtered, list_picking)
    
    df_grouped = group_data(df_filtered)
    df_grouped = update_comment(df_grouped, picking_dict)
    df_grouped = unify_orders(df_grouped, dict_zones)
    
    df_grouped= create_roadmap(df_grouped, df_aux_hruta)
    
    return df_grouped

# Construcción de entorno
st.set_page_config(page_title="My Routes", page_icon=":anger:", layout="wide")
# Inyectar CSS personalizado para ajustar los márgenes
st.markdown(
    """
    <style>
    .css-18e3th9 {
        padding-top: 0.5rem;
        padding-bottom: 0.5rem;
        padding-left: 1rem;
        padding-right: 1rem;
    }
    .css-1d391kg {
        padding-top: 0.5rem;
        padding-right: 1rem;
        padding-bottom: 0.5rem;
        padding-left: 1rem;
    }
    </style>
    """,
    unsafe_allow_html=True
)
hide_st_style = """
                <style>
                    #MainMenu {visibility: hidden;}
                    footer {visibility: hidden;}
                    header {visibility: hidden;}       
                </style>
            """
st.markdown(hide_st_style, unsafe_allow_html=True)
menu =["Sucursales"]
choice = st.sidebar.write("")


def menu():
    st.header("My Routes")
    st.divider()
    value_date = st.date_input("Enter you registration Date", value=day_process, format="YYYY-MM-DD")
    file_csv = st.file_uploader("Please upload an csv", type=["csv"])  
    if file_csv is not None:
        data = model_process(file_csv,value_date)
        SUCURSALES = data["Personalizado 2"].unique()
        SUCURSALES_SELECTED = st.sidebar.multiselect("Seleccionar sucursal", SUCURSALES, SUCURSALES )
        mask_sucursales = data["Personalizado 2"].isin(SUCURSALES_SELECTED)
        data = data[mask_sucursales]
        col1, col2, col3, col4 = st.columns(4)
        orders = data['Nombre'].count()
        units = data['Cantidad de bultos'].sum()
        units_order = units / orders
        col1.metric("Pedidos", f"{orders} #")
        col2.metric("Cajas", f"{units} UN")
        col3.metric("Min.Paradas", f"{data['Tiempo en destino (min)'].sum()} Min.")
        col4.metric("Reenviados", f"{data['Personalizado 1'].str.contains('REE', case=False).sum()} #")   
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Reprogramados", f"{data['Personalizado 1'].str.contains('REP', case=False).sum()} #")    
        col2.metric("Unificados", f"{data['Personalizado 1'].str.contains('Unifi', case=False).sum()} #") 
        col3.metric("Dirección No Verificada", f"{data['Personalizado 1'].str.contains('No Verif', case=False).sum()} #")            
        st.divider() 
        if len(SUCURSALES_SELECTED) == 1:
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                data.to_excel(writer, sheet_name='Ruteo', index=False)
            st.sidebar.download_button(label="Download Excel", data=buffer.getvalue(), file_name="datmaset.xlsx", mime="application/vnd.ms-excel")
        st.dataframe(data)    
       
  




       
       
       
    
menu()
