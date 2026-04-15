"""
Dashboard NKL Costos — Streamlit
Conecta a Google Sheets y muestra:
  1. Vista General (cierre mensual, distribución de gastos)
  2. Por Orden/Proyecto (desglose individual)
  3. Nómina (por colaborador, por proyecto)
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from collections import defaultdict
import re
import json

import gspread
from google.oauth2.service_account import Credentials

# ─── CONFIG ──────────────────────────────────────────────────────────────────
SPREADSHEET_ID = '1Xwe6rs2EirZ2LiiutXvML6aCjjWJug0pcUhMcsqaVlY'
HOJAS_FUENTE = ['CAJA 1', 'CAJA 2', 'HSBC-FISCAL', 'TC VERO', 'TC  EXT', 'TC EMPRESARIAL']
HOJA_CATALOGO = 'CLASIFICACIONES-R'

MESES = ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
         'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre']

CLASIFICACIONES_EXCLUIR = {
    'SALDO INCIAL', 'SALDO INICIAL', 'INICIAL',
    'TRASPASO', 'TRASPASO ENTRE CUENTAS',
    'PAGO', 'TRANSFERENCIA',
}
CLASIFICACIONES_INGRESO = {
    'VENTA': 'Ventas',
    'DEVOLUCION DE NOMINA': 'Devolución de nómina',
    'DEVOLUCIÓN DE NÓMINA': 'Devolución de nómina',
    'DEVOLUCION INFONAVIT': 'Devolución de Infonavit',
    'DEVOLUCIÓN INFONAVIT': 'Devolución de Infonavit',
}

# Columnas hojas fuente (libro diario)
COL_DIA, COL_MES, COL_ANO = 0, 1, 2
COL_ORDEN, COL_CONCEPTO = 3, 5
COL_TIPO, COL_SUB, COL_IVA = 7, 8, 9
COL_T_INGRESO, COL_T_EGRESO, COL_T_PAGO = 10, 11, 12
COL_CLASIFICACION = 16

# Columnas CONTROL DE GASTOS
G_ORDEN, G_DIA, G_MES, G_ANO = 0, 1, 2, 3
G_FOLIO, G_PROVEEDOR, G_CANTIDAD, G_CONCEPTO = 4, 5, 6, 7
G_CANT_PARTIDA, G_UNIDAD, G_P_UNITARIO, G_IMPORTE = 12, 13, 14, 15
G_SUBCATEGORIA = 18

# Columnas CONTROL DE NÓMINA
N_SEMANA, N_DIA, N_MES, N_ANO = 3, 4, 5, 6
N_COLABORADOR, N_TIPO, N_COSTO_HORA, N_HORAS = 7, 8, 9, 10
N_ORDEN, N_TOTAL = 11, 12


# ─── HELPERS ─────────────────────────────────────────────────────────────────

def parse_monto(valor):
    if not valor or valor in ('-', '**', '#REF!'):
        return 0.0
    limpio = valor.replace('$', '').replace(',', '').replace(' ', '').strip()
    if not limpio:
        return 0.0
    try:
        return float(limpio)
    except ValueError:
        return 0.0


def normalizar(texto):
    return re.sub(r'\s+', ' ', texto.strip().upper())


def fmt(valor):
    """Formato moneda MXN."""
    return f"${valor:,.2f}"


def conectar_sheets():
    """Conecta a Google Sheets usando Streamlit Secrets o archivo local."""
    scopes = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/spreadsheets.readonly'
    ]
    try:
        # Streamlit Cloud: lee de st.secrets
        creds_info = dict(st.secrets["gcp_service_account"])
        creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    except (KeyError, FileNotFoundError):
        # Local: lee de credentials.json
        creds = Credentials.from_service_account_file('credentials.json', scopes=scopes)

    client = gspread.authorize(creds)
    return client.open_by_key(SPREADSHEET_ID)


# ─── CARGA DE DATOS (cacheada) ──────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner="Leyendo datos de Google Sheets...")
def cargar_datos():
    """Lee todas las hojas y retorna datos procesados."""
    sh = conectar_sheets()

    # ── Catálogo ──
    ws_cat = sh.worksheet(HOJA_CATALOGO)
    rows_cat = ws_cat.get_all_values()
    catalogo = {}
    for row in rows_cat[1:]:
        if len(row) < 4 or not row[3].strip():
            continue
        sub_norm = normalizar(row[3])
        catalogo[sub_norm] = {
            'directo_indirecto': row[0].strip(),
            'fijo_variable': row[1].strip(),
            'categoria_mayor': row[2].strip(),
            'subcategoria': row[3].strip(),
        }

    # ── Hojas fuente (libro diario) ──
    registros_libro = []
    for hoja in HOJAS_FUENTE:
        ws = sh.worksheet(hoja)
        rows = ws.get_all_values()
        for i, row in enumerate(rows[1:], start=2):
            if len(row) < 17:
                continue
            try:
                mes = int(row[COL_MES])
                ano = int(row[COL_ANO])
            except (ValueError, IndexError):
                continue
            clasif_raw = row[COL_CLASIFICACION].strip()
            if not clasif_raw:
                continue
            if clasif_raw.startswith('$') or re.match(r'^[\d,\.]+$', clasif_raw):
                continue
            registros_libro.append({
                'hoja': hoja, 'fila': i,
                'mes': mes, 'ano': ano,
                'orden': row[COL_ORDEN],
                'concepto': row[COL_CONCEPTO],
                'sub': parse_monto(row[COL_SUB]),
                'iva': parse_monto(row[COL_IVA]),
                't_ingreso': parse_monto(row[COL_T_INGRESO]),
                't_egreso': parse_monto(row[COL_T_EGRESO]),
                'clasificacion': normalizar(clasif_raw),
                'clasificacion_raw': clasif_raw,
            })

    # ── CONTROL DE GASTOS ──
    ws_g = sh.worksheet('CONTROL DE GASTOS')
    rows_g = ws_g.get_all_values()
    gastos = []
    for row in rows_g[1:]:
        orden = row[G_ORDEN].strip()
        if not orden:
            continue
        importe = parse_monto(row[G_IMPORTE])
        if importe == 0:
            continue
        gastos.append({
            'orden': orden,
            'fecha': f"{row[G_DIA]}/{row[G_MES]}/{row[G_ANO]}",
            'folio': row[G_FOLIO],
            'proveedor': row[G_PROVEEDOR],
            'concepto': row[G_CONCEPTO],
            'cantidad': row[G_CANT_PARTIDA],
            'unidad': row[G_UNIDAD],
            'p_unitario': row[G_P_UNITARIO],
            'importe': importe,
            'subcategoria': row[G_SUBCATEGORIA].strip().upper() if len(row) > G_SUBCATEGORIA and row[G_SUBCATEGORIA].strip() else 'SIN CLASIFICAR',
        })

    # ── CONTROL DE NÓMINA ──
    ws_n = sh.worksheet('CONTROL DE NÓMINA')
    rows_n = ws_n.get_all_values()
    nomina = []
    for row in rows_n[1:]:
        if len(row) <= N_TOTAL:
            continue
        orden = row[N_ORDEN].strip()
        if not orden:
            continue
        total = parse_monto(row[N_TOTAL])
        if total == 0:
            continue
        nomina.append({
            'orden': orden,
            'semana': row[N_SEMANA],
            'fecha': f"{row[N_DIA]}/{row[N_MES]}/{row[N_ANO]}",
            'colaborador': row[N_COLABORADOR],
            'tipo': row[N_TIPO].strip(),
            'costo_hora': row[N_COSTO_HORA],
            'horas': row[N_HORAS],
            'total': total,
        })

    return catalogo, registros_libro, gastos, nomina


# ─── PROCESAMIENTO ───────────────────────────────────────────────────────────

def calcular_cierre(registros, ano, catalogo):
    """Calcula cierre mensual para un año."""
    filas = []
    for reg in registros:
        if reg['ano'] != ano:
            continue
        clasif = reg['clasificacion']
        if clasif in CLASIFICACIONES_EXCLUIR:
            continue

        es_ingreso = clasif in CLASIFICACIONES_INGRESO
        cat_info = catalogo.get(clasif, {})

        filas.append({
            'mes': reg['mes'],
            'mes_nombre': MESES[reg['mes'] - 1] if 1 <= reg['mes'] <= 12 else '?',
            'clasificacion': reg['clasificacion_raw'],
            'tipo': 'Ingreso' if es_ingreso else 'Egreso',
            'categoria': CLASIFICACIONES_INGRESO.get(clasif, cat_info.get('categoria_mayor', 'Otro')),
            'subcategoria': cat_info.get('subcategoria', reg['clasificacion_raw']),
            'directo_indirecto': cat_info.get('directo_indirecto', ''),
            'fijo_variable': cat_info.get('fijo_variable', ''),
            'monto': reg['t_ingreso'] if es_ingreso else reg['t_egreso'],
            'subtotal': reg['sub'],
            'iva': reg['iva'],
        })
    return pd.DataFrame(filas) if filas else pd.DataFrame()


# ─── PÁGINAS ─────────────────────────────────────────────────────────────────

def pagina_general(registros, catalogo):
    st.header("Vista General — Cierre Mensual")

    anos_disponibles = sorted({r['ano'] for r in registros})
    if not anos_disponibles:
        st.warning("No hay datos disponibles.")
        return

    ano = st.selectbox("Año", anos_disponibles, index=len(anos_disponibles) - 1,
                       format_func=lambda x: f"20{x}")

    df = calcular_cierre(registros, ano, catalogo)
    if df.empty:
        st.info(f"Sin datos para 20{ano}")
        return

    # ── Métricas principales ──
    total_ing = df[df['tipo'] == 'Ingreso']['monto'].sum()
    total_eg = df[df['tipo'] == 'Egreso']['monto'].sum()
    resultado = total_ing - total_eg

    c1, c2, c3 = st.columns(3)
    c1.metric("Ingresos (Total)", fmt(total_ing))
    c2.metric("Egresos (Total)", fmt(total_eg))
    c3.metric("Resultado", fmt(resultado),
              delta=f"{resultado/total_ing*100:.1f}%" if total_ing > 0 else None,
              delta_color="normal" if resultado >= 0 else "inverse")

    # ── Desglose Subtotal / IVA ──
    df_ing = df[df['tipo'] == 'Ingreso']
    df_egr = df[df['tipo'] == 'Egreso']

    sub_ing = df_ing['subtotal'].sum()
    iva_ing = df_ing['iva'].sum()
    sub_eg = df_egr['subtotal'].sum()
    iva_eg = df_egr['iva'].sum()

    st.subheader("Desglose: Subtotal e IVA")
    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown("**Ingresos**")
        m1, m2, m3 = st.columns(3)
        m1.metric("Subtotal", fmt(sub_ing))
        m2.metric("IVA", fmt(iva_ing))
        m3.metric("Total", fmt(total_ing))
    with col_b:
        st.markdown("**Egresos**")
        m1, m2, m3 = st.columns(3)
        m1.metric("Subtotal", fmt(sub_eg))
        m2.metric("IVA", fmt(iva_eg))
        m3.metric("Total", fmt(total_eg))

    # Tabla mensual de Subtotal / IVA / Total
    with st.expander("Ver desglose mensual de Subtotal / IVA"):
        filas_desg = []
        for mes_num in sorted(df['mes'].unique()):
            dm = df[df['mes'] == mes_num]
            di = dm[dm['tipo'] == 'Ingreso']
            de = dm[dm['tipo'] == 'Egreso']
            filas_desg.append({
                'Mes': MESES[mes_num - 1] if 1 <= mes_num <= 12 else '?',
                'Ing. Subtotal': di['subtotal'].sum(),
                'Ing. IVA': di['iva'].sum(),
                'Ing. Total': di['monto'].sum(),
                'Egr. Subtotal': de['subtotal'].sum(),
                'Egr. IVA': de['iva'].sum(),
                'Egr. Total': de['monto'].sum(),
            })
        df_desg = pd.DataFrame(filas_desg)
        # Fila de totales
        totales_desg = {col: df_desg[col].sum() if col != 'Mes' else 'TOTAL' for col in df_desg.columns}
        df_desg = pd.concat([df_desg, pd.DataFrame([totales_desg])], ignore_index=True)
        cols_fmt = {c: '${:,.2f}' for c in df_desg.columns if c != 'Mes'}
        st.dataframe(df_desg.style.format(cols_fmt), use_container_width=True, hide_index=True)

    # ── Ingresos vs Egresos por mes ──
    st.subheader("Ingresos vs Egresos por mes")
    mensual = df.groupby(['mes', 'mes_nombre', 'tipo'])['monto'].sum().reset_index()
    mensual = mensual.sort_values('mes')

    fig_barras = px.bar(mensual, x='mes_nombre', y='monto', color='tipo',
                        barmode='group',
                        color_discrete_map={'Ingreso': '#2ecc71', 'Egreso': '#e74c3c'},
                        labels={'monto': 'Monto ($)', 'mes_nombre': 'Mes', 'tipo': ''},
                        text_auto=',.0f')
    fig_barras.update_layout(xaxis_title='', yaxis_title='', yaxis_tickformat='$,.0f',
                             legend=dict(orientation='h', y=1.1))
    st.plotly_chart(fig_barras, use_container_width=True)

    # ── Resultado acumulado ──
    resultado_mes = df.groupby(['mes', 'mes_nombre']).apply(
        lambda g: g[g['tipo'] == 'Ingreso']['monto'].sum() - g[g['tipo'] == 'Egreso']['monto'].sum(),
        include_groups=False
    ).reset_index(name='resultado')
    resultado_mes = resultado_mes.sort_values('mes')
    resultado_mes['acumulado'] = resultado_mes['resultado'].cumsum()

    fig_acum = go.Figure()
    fig_acum.add_trace(go.Bar(x=resultado_mes['mes_nombre'], y=resultado_mes['resultado'],
                              name='Resultado mensual',
                              marker_color=['#2ecc71' if v >= 0 else '#e74c3c'
                                            for v in resultado_mes['resultado']]))
    fig_acum.add_trace(go.Scatter(x=resultado_mes['mes_nombre'], y=resultado_mes['acumulado'],
                                  name='Acumulado', mode='lines+markers',
                                  line=dict(color='#3498db', width=3)))
    fig_acum.update_layout(title='Resultado mensual y acumulado',
                           yaxis_tickformat='$,.0f',
                           legend=dict(orientation='h', y=1.12))
    st.plotly_chart(fig_acum, use_container_width=True)

    # ── Distribución de egresos ──
    st.subheader("Distribución de egresos")
    egresos = df[df['tipo'] == 'Egreso']

    col1, col2 = st.columns(2)

    with col1:
        por_cat = egresos.groupby('categoria')['monto'].sum().sort_values(ascending=False).reset_index()
        if not por_cat.empty:
            fig_pie = px.pie(por_cat, values='monto', names='categoria',
                             title='Por categoría mayor', hole=0.35)
            fig_pie.update_traces(textinfo='percent+label', textposition='outside')
            fig_pie.update_layout(showlegend=False)
            st.plotly_chart(fig_pie, use_container_width=True)

    with col2:
        por_tipo = egresos.groupby('directo_indirecto')['monto'].sum().reset_index()
        por_tipo = por_tipo[por_tipo['directo_indirecto'] != '']
        if not por_tipo.empty:
            fig_tipo = px.pie(por_tipo, values='monto', names='directo_indirecto',
                              title='Directo vs Indirecto', hole=0.35,
                              color_discrete_map={'Directo': '#e67e22', 'Indirecto': '#9b59b6'})
            fig_tipo.update_traces(textinfo='percent+label', textposition='outside')
            fig_tipo.update_layout(showlegend=False)
            st.plotly_chart(fig_tipo, use_container_width=True)

    # ── Top subcategorías ──
    st.subheader("Top 15 subcategorías de egreso")
    top_sub = egresos.groupby('subcategoria')['monto'].sum().sort_values(ascending=True).tail(15).reset_index()
    fig_top = px.bar(top_sub, x='monto', y='subcategoria', orientation='h',
                     text_auto='$,.0f', color_discrete_sequence=['#e74c3c'])
    fig_top.update_layout(xaxis_tickformat='$,.0f', xaxis_title='', yaxis_title='', height=500)
    st.plotly_chart(fig_top, use_container_width=True)

    # ── Tabla detallada ──
    with st.expander("Ver tabla completa del cierre"):
        pivot = df.pivot_table(index='subcategoria', columns='mes_nombre',
                               values='monto', aggfunc='sum', fill_value=0,
                               margins=True, margins_name='TOTAL')
        cols_orden = [m for m in MESES if m in pivot.columns] + ['TOTAL']
        pivot = pivot[cols_orden]
        st.dataframe(pivot.style.format("${:,.2f}"), use_container_width=True, height=600)


def pagina_ordenes(df_gastos, df_nomina):
    st.header("Costos por Orden / Proyecto")

    if df_gastos.empty and df_nomina.empty:
        st.warning("No hay datos en las hojas de control.")
        return

    # Todas las órdenes
    ordenes_g = set(df_gastos['orden'].unique()) if not df_gastos.empty else set()
    ordenes_n = set(df_nomina['orden'].unique()) if not df_nomina.empty else set()
    todas = sorted(ordenes_g | ordenes_n)

    # Calcular totales
    resumen = []
    for orden in todas:
        t_g = df_gastos[df_gastos['orden'] == orden]['importe'].sum() if not df_gastos.empty else 0
        t_n = df_nomina[df_nomina['orden'] == orden]['total'].sum() if not df_nomina.empty else 0
        resumen.append({'Orden': orden, 'Gastos': t_g, 'Nómina': t_n, 'Total': t_g + t_n})
    df_resumen = pd.DataFrame(resumen).sort_values('Total', ascending=False)

    # ── Métricas ──
    c1, c2, c3 = st.columns(3)
    c1.metric("Órdenes", len(todas))
    c2.metric("Total Gastos", fmt(df_resumen['Gastos'].sum()))
    c3.metric("Total Nómina", fmt(df_resumen['Nómina'].sum()))

    # Top 20
    top20 = df_resumen.head(20)
    fig_rank = go.Figure()
    fig_rank.add_trace(go.Bar(name='Gastos', y=top20['Orden'], x=top20['Gastos'],
                              orientation='h', marker_color='#e67e22'))
    fig_rank.add_trace(go.Bar(name='Nómina', y=top20['Orden'], x=top20['Nómina'],
                              orientation='h', marker_color='#3498db'))
    fig_rank.update_layout(barmode='stack', title='Top 20 órdenes por costo total',
                           xaxis_tickformat='$,.0f', height=600,
                           yaxis=dict(autorange='reversed'),
                           legend=dict(orientation='h', y=1.08))
    st.plotly_chart(fig_rank, use_container_width=True)

    # ── Detalle individual ──
    st.subheader("Detalle por orden")
    opciones = (
        sorted([o for o in todas if o.startswith('O-')],
               key=lambda x: int(re.search(r'\d+', x).group()) if re.search(r'\d+', x) else 0,
               reverse=True)
        + sorted([o for o in todas if o.startswith('DOM.')])
        + sorted([o for o in todas if not o.startswith('O-') and not o.startswith('DOM.')])
    )

    orden_sel = st.selectbox("Seleccionar orden", opciones)

    if orden_sel:
        g_sel = df_gastos[df_gastos['orden'] == orden_sel] if not df_gastos.empty else pd.DataFrame()
        n_sel = df_nomina[df_nomina['orden'] == orden_sel] if not df_nomina.empty else pd.DataFrame()

        t_gastos = g_sel['importe'].sum() if not g_sel.empty else 0
        t_nomina = n_sel['total'].sum() if not n_sel.empty else 0
        t_total = t_gastos + t_nomina

        c1, c2, c3 = st.columns(3)
        c1.metric("Gastos", fmt(t_gastos))
        c2.metric("Nómina", fmt(t_nomina))
        c3.metric("Costo Total", fmt(t_total))

        if t_total > 0:
            col1, col2 = st.columns(2)
            with col1:
                comp_data = []
                if not g_sel.empty:
                    for sub, monto in g_sel.groupby('subcategoria')['importe'].sum().items():
                        comp_data.append({'Concepto': sub, 'Monto': monto})
                if not n_sel.empty:
                    for tipo, monto in n_sel.groupby('tipo')['total'].sum().items():
                        comp_data.append({'Concepto': f'Nómina: {tipo}', 'Monto': monto})
                if comp_data:
                    df_comp = pd.DataFrame(comp_data)
                    fig_comp = px.pie(df_comp, values='Monto', names='Concepto',
                                     title='Composición del costo', hole=0.35)
                    fig_comp.update_traces(textinfo='percent+value', textposition='outside',
                                           texttemplate='%{label}<br>%{value:$,.0f}')
                    fig_comp.update_layout(showlegend=False)
                    st.plotly_chart(fig_comp, use_container_width=True)

            with col2:
                if not n_sel.empty:
                    por_colab = n_sel.groupby('colaborador')['total'].sum().sort_values(ascending=True).reset_index()
                    fig_colab = px.bar(por_colab, x='total', y='colaborador', orientation='h',
                                       title='Costo por colaborador',
                                       text_auto='$,.0f', color_discrete_sequence=['#3498db'])
                    fig_colab.update_layout(xaxis_tickformat='$,.0f', xaxis_title='', yaxis_title='')
                    st.plotly_chart(fig_colab, use_container_width=True)

        if not g_sel.empty:
            st.markdown("**Detalle de gastos**")
            tabla_g = g_sel[['fecha', 'folio', 'proveedor', 'concepto', 'subcategoria',
                             'cantidad', 'unidad', 'p_unitario', 'importe']].copy()
            tabla_g.columns = ['Fecha', 'Folio', 'Proveedor', 'Concepto', 'Subcategoría',
                               'Cant.', 'Unidad', 'P.Unit.', 'Importe']
            st.dataframe(tabla_g.style.format({'Importe': '${:,.2f}'}),
                         use_container_width=True, hide_index=True)

        if not n_sel.empty:
            st.markdown("**Detalle de nómina**")
            tabla_n = n_sel[['semana', 'fecha', 'colaborador', 'tipo',
                             'costo_hora', 'horas', 'total']].copy()
            tabla_n.columns = ['Semana', 'Fecha', 'Colaborador', 'Tipo',
                               'Costo/Hr', 'Horas', 'Total']
            st.dataframe(tabla_n.style.format({'Total': '${:,.2f}'}),
                         use_container_width=True, hide_index=True)

    with st.expander("Ver tabla resumen de todas las órdenes"):
        st.dataframe(
            df_resumen.style.format({'Gastos': '${:,.2f}', 'Nómina': '${:,.2f}', 'Total': '${:,.2f}'}),
            use_container_width=True, hide_index=True, height=600
        )


def pagina_nomina(df_nomina):
    st.header("Análisis de Nómina Operativa")

    if df_nomina.empty:
        st.warning("No hay datos de nómina.")
        return

    total = df_nomina['total'].sum()
    n_colaboradores = df_nomina['colaborador'].nunique()
    n_ordenes = df_nomina['orden'].nunique()

    c1, c2, c3 = st.columns(3)
    c1.metric("Total Nómina", fmt(total))
    c2.metric("Colaboradores", n_colaboradores)
    c3.metric("Órdenes/Proyectos", n_ordenes)

    col1, col2 = st.columns(2)

    with col1:
        por_colab = df_nomina.groupby('colaborador')['total'].sum().sort_values(ascending=True).reset_index()
        fig_c = px.bar(por_colab, x='total', y='colaborador', orientation='h',
                       title='Costo total por colaborador',
                       text_auto='$,.0f', color_discrete_sequence=['#3498db'])
        fig_c.update_layout(xaxis_tickformat='$,.0f', xaxis_title='', yaxis_title='',
                            height=max(400, len(por_colab) * 28))
        st.plotly_chart(fig_c, use_container_width=True)

    with col2:
        por_tipo = df_nomina.groupby('tipo')['total'].sum().reset_index()
        fig_t = px.pie(por_tipo, values='total', names='tipo',
                       title='Distribución por tipo', hole=0.35)
        fig_t.update_traces(textinfo='percent+value',
                            texttemplate='%{label}<br>%{value:$,.0f}')
        fig_t.update_layout(showlegend=False)
        st.plotly_chart(fig_t, use_container_width=True)

    # Heatmap
    st.subheader("Mapa de calor: Colaborador x Orden")
    top_ordenes = df_nomina.groupby('orden')['total'].sum().sort_values(ascending=False).head(20).index
    df_heat = df_nomina[df_nomina['orden'].isin(top_ordenes)]
    pivot_heat = df_heat.pivot_table(index='colaborador', columns='orden',
                                      values='total', aggfunc='sum', fill_value=0)
    col_order = pivot_heat.sum().sort_values(ascending=False).index
    pivot_heat = pivot_heat[col_order]

    fig_heat = px.imshow(pivot_heat, text_auto='$,.0f', aspect='auto',
                         color_continuous_scale='Blues',
                         labels=dict(x='Orden', y='Colaborador', color='Monto'))
    fig_heat.update_layout(height=max(400, len(pivot_heat) * 35))
    st.plotly_chart(fig_heat, use_container_width=True)

    # Detalle por colaborador
    st.subheader("Detalle por colaborador")
    colaboradores = sorted(df_nomina['colaborador'].unique())
    colab_sel = st.selectbox("Seleccionar colaborador", colaboradores)

    if colab_sel:
        df_c = df_nomina[df_nomina['colaborador'] == colab_sel]
        st.metric("Total", fmt(df_c['total'].sum()))

        por_orden = df_c.groupby('orden')['total'].sum().sort_values(ascending=False).reset_index()
        por_orden.columns = ['Orden', 'Total']
        fig_o = px.bar(por_orden, x='Orden', y='Total', text_auto='$,.0f',
                       color_discrete_sequence=['#2ecc71'])
        fig_o.update_layout(yaxis_tickformat='$,.0f', xaxis_title='', yaxis_title='')
        st.plotly_chart(fig_o, use_container_width=True)

        st.dataframe(
            df_c[['semana', 'fecha', 'orden', 'tipo', 'horas', 'total']].rename(
                columns={'semana': 'Semana', 'fecha': 'Fecha', 'orden': 'Orden',
                         'tipo': 'Tipo', 'horas': 'Horas', 'total': 'Total'}
            ).style.format({'Total': '${:,.2f}'}),
            use_container_width=True, hide_index=True
        )


# ─── MAIN ────────────────────────────────────────────────────────────────────

def main():
    st.set_page_config(page_title="NKL Costos", page_icon="📊", layout="wide")

    st.title("NKL Costos — Dashboard")
    st.caption("Datos en tiempo real desde Google Sheets")

    pagina = st.sidebar.radio("Navegación", [
        "📊 Vista General",
        "🏗️ Por Orden / Proyecto",
        "👷 Nómina",
    ])

    if st.sidebar.button("🔄 Recargar datos"):
        st.cache_data.clear()
        st.rerun()

    catalogo, registros, gastos, nomina = cargar_datos()
    df_gastos = pd.DataFrame(gastos) if gastos else pd.DataFrame()
    df_nomina = pd.DataFrame(nomina) if nomina else pd.DataFrame()

    if pagina == "📊 Vista General":
        pagina_general(registros, catalogo)
    elif pagina == "🏗️ Por Orden / Proyecto":
        pagina_ordenes(df_gastos, df_nomina)
    elif pagina == "👷 Nómina":
        pagina_nomina(df_nomina)


if __name__ == '__main__':
    main()
