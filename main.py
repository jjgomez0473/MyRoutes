import streamlit as st

st.header("My Routes")
st.title("Uploading Files")
st.markdown("---")
file_csv = st.file_uploader("Please upload an csv", type=["csv"])
if file_csv is not None:
    # TODO cargar archivo
    a = 10
    
value_date = st.date_input("Enter yupy registration Date")


