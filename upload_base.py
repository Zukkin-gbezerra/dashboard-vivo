#!/usr/bin/env python3
"""
upload_base.py — Script para processar nova base de dados e atualizar o dashboard.

Uso:
  python upload_base.py caminho/para/nova_base.xlsx

Requisitos:
  pip install pandas openpyxl supabase requests
"""

import sys
import json
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

# ===================== CONFIG =====================
SUPABASE_URL = 'https://srzolfkywvilcqbkmblt.supabase.co'
SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InNyem9sZmt5d3ZpbGNxYmttYmx0Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzUwODA1NTQsImV4cCI6MjA5MDY1NjU1NH0.IzXb4Te9h8OPQds2bmBQlw8Lho0d4stX4bRPF9v65sc'

PAYMENT_COLS = {
    'pix': 'À Vista (PIX)',
    'debito': 'Débito',
    'boleto': 'Boleto',
    'cartao1x': 'Cartão (1x)',
    'cartaoPrazo': 'Cartão (Prazo sem Juros)',
    'cartaoPrazoJuros': 'Cartão (Prazo com Juros)',
    'cartaoProprio': 'Cartão Próprio (Prazo sem Juros)',
}

LOJAS_AMBOS = ['Casas Bahia', 'Fast Shop', 'Lojas Americanas', 'Magalu', 'Samsung', 'iPlace']

def agg_stats(vals):
    vals = vals.dropna()
    if len(vals) == 0:
        return None
    return {
        'mediana': round(float(vals.median()), 2),
        'media': round(float(vals.mean()), 2),
        'moda': round(float(vals.mode().iloc[0]) if len(vals.mode()) > 0 else vals.median(), 2),
        'min': round(float(vals.min()), 2),
        'max': round(float(vals.max()), 2),
        'count': int(len(vals))
    }

def process_xlsx(filepath):
    print(f"📂 Carregando {filepath}...")
    df = pd.read_excel(filepath, header=0)
    df['canal'] = df['Local'].apply(lambda x: 'Online' if 'Site' in str(x) else 'Físico')
    df['Loja'] = df['Loja'].replace({'Magazine Luiza': 'Magalu'})
    df['Data'] = pd.to_datetime(df['Data'])

    all_pay_cols = list(PAYMENT_COLS.values())
    df['todos'] = df[all_pay_cols].mean(axis=1)
    all_keys = list(PAYMENT_COLS.keys()) + ['todos']

    def get_col(pk):
        return 'todos' if pk == 'todos' else PAYMENT_COLS[pk]

    print(f"✅ {len(df)} registros carregados")
    print(f"📅 Período: {df['Data'].min().date()} a {df['Data'].max().date()}")

    # Build all data structures
    print("⚙️  Processando dados...")

    # Products
    prods_base = []
    prods_stats = {pk: [] for pk in all_keys}

    prod_keys = df.groupby(['Produto', 'Marca',
                            'Departamento (Nível 1)', 'Departamento (Nível 2)',
                            'Departamento (Nível 3)']).size().reset_index()

    for _, row in prod_keys.iterrows():
        prods_base.append({
            'produto': row['Produto'],
            'marca': str(row['Marca']),
            'd1': str(row['Departamento (Nível 1)']),
            'd2': str(row['Departamento (Nível 2)']),
            'd3': str(row['Departamento (Nível 3)']),
        })

    for pk in all_keys:
        col = get_col(pk)
        tmp = df.copy()
        tmp['_val'] = tmp[col]
        for _, base in enumerate(prods_base):
            prod_df = tmp[tmp['Produto'] == base['produto']]
            o = agg_stats(prod_df[prod_df['canal'] == 'Online']['_val'])
            f = agg_stats(prod_df[prod_df['canal'] == 'Físico']['_val'])
            prods_stats[pk].append({'O': o, 'F': f})

    # Lojas
    lojas_data = []
    for pk in all_keys:
        col = get_col(pk)
        tmp = df.copy()
        tmp['_val'] = tmp[col]
        df_lojas = tmp[tmp['Loja'].isin(LOJAS_AMBOS)]
        loja_map = {}
        for (loja, canal, uf), grp in df_lojas.groupby(['Loja', 'canal', 'Estado (UF)']):
            s = agg_stats(grp['_val'])
            if s:
                if loja not in loja_map:
                    loja_map[loja] = {'loja': loja, 'ufs': {}}
                if uf not in loja_map[loja]['ufs']:
                    loja_map[loja]['ufs'][uf] = {}
                loja_map[loja]['ufs'][uf][canal] = s
        for (loja, canal), grp in df_lojas.groupby(['Loja', 'canal']):
            s = agg_stats(grp['_val'])
            if s:
                if loja not in loja_map:
                    loja_map[loja] = {'loja': loja, 'ufs': {}}
                if 'GERAL' not in loja_map[loja]:
                    loja_map[loja]['GERAL'] = {}
                loja_map[loja]['GERAL'][canal] = s
        if not lojas_data:
            lojas_data = list(loja_map.values())

    # TS, UF, DEPT
    DTS, DUF, DDEPT = {}, {}, {}
    for pk in all_keys:
        col = get_col(pk)
        tmp = df.copy()
        tmp['_val'] = tmp[col]
        tmp['semana'] = tmp['Data'].dt.to_period('W').astype(str)
        ts = []
        for (sem, canal), grp in tmp.groupby(['semana', 'canal']):
            s = agg_stats(grp['_val'])
            if s:
                ts.append({'semana': str(sem), 'canal': canal, **s})
        DTS[pk] = ts

        uf = []
        for (uf_val, canal), grp in tmp.groupby(['Estado (UF)', 'canal']):
            s = agg_stats(grp['_val'])
            if s:
                uf.append({'uf': str(uf_val), 'canal': canal, **s})
        DUF[pk] = uf

        dept = []
        for (d1, d2, canal), grp in tmp.groupby(['Departamento (Nível 1)', 'Departamento (Nível 2)', 'canal']):
            s = agg_stats(grp['_val'])
            if s:
                dept.append({'d1': str(d1), 'd2': str(d2), 'canal': canal, **s})
        DDEPT[pk] = dept

    # Loja comparable data
    DLOJA_COMP = {}
    DLOJA_CHARTS = {'todos_detail': {}, 'comp_detail': {}}

    for pk in all_keys:
        col = get_col(pk)
        tmp = df.copy()
        tmp['_val'] = tmp[col]
        loja_comp = {}
        for loja in LOJAS_AMBOS:
            df_loja = tmp[tmp['Loja'] == loja]
            p_online = set(df_loja[df_loja['canal'] == 'Online']['Produto'].unique())
            p_fisico = set(df_loja[df_loja['canal'] == 'Físico']['Produto'].unique())
            p_both = p_online & p_fisico
            df_comp = df_loja[df_loja['Produto'].isin(p_both)]
            o = agg_stats(df_comp[df_comp['canal'] == 'Online']['_val'])
            f = agg_stats(df_comp[df_comp['canal'] == 'Físico']['_val'])
            ufs_comp = {}
            for uf, grp in df_comp.groupby('Estado (UF)'):
                uo = agg_stats(grp[grp['canal'] == 'Online']['_val'])
                uf_ = agg_stats(grp[grp['canal'] == 'Físico']['_val'])
                if uo or uf_:
                    ufs_comp[str(uf)] = {}
                    if uo: ufs_comp[str(uf)]['Online'] = uo
                    if uf_: ufs_comp[str(uf)]['Físico'] = uf_
            loja_comp[loja] = {'n_comparaveis': len(p_both), 'GERAL': {}, 'ufs': ufs_comp}
            if o: loja_comp[loja]['GERAL']['Online'] = o
            if f: loja_comp[loja]['GERAL']['Físico'] = f
        DLOJA_COMP[pk] = loja_comp

        # Charts todos_detail and comp_detail
        todos_detail = {}
        comp_detail = {}
        for loja in LOJAS_AMBOS:
            df_loja = tmp[tmp['Loja'] == loja]
            o_global = agg_stats(df_loja[df_loja['canal'] == 'Online']['_val'])
            f_by_uf = {}
            for uf, grp in df_loja[df_loja['canal'] == 'Físico'].groupby('Estado (UF)'):
                s = agg_stats(grp['_val'])
                if s: f_by_uf[str(uf)] = s
            todos_detail[loja] = {'online_global': o_global, 'fisico_by_uf': f_by_uf}

            p_online = set(df_loja[df_loja['canal'] == 'Online']['Produto'].unique())
            p_fisico = set(df_loja[df_loja['canal'] == 'Físico']['Produto'].unique())
            prod_data = []
            for prod in p_online & p_fisico:
                df_prod = df_loja[df_loja['Produto'] == prod]
                po = agg_stats(df_prod[df_prod['canal'] == 'Online']['_val'])
                pf = agg_stats(df_prod[df_prod['canal'] == 'Físico']['_val'])
                if po and pf:
                    diff = (pf['mediana'] - po['mediana']) / po['mediana'] if po['mediana'] else 0
                    prod_data.append({'produto': prod, 'Online': po, 'Físico': pf, 'diff': round(diff, 4), 'total_count': po['count'] + pf['count']})
            prod_data.sort(key=lambda x: x['total_count'], reverse=True)
            comp_detail[loja] = prod_data[:20]
        DLOJA_CHARTS['todos_detail'][pk] = todos_detail
        DLOJA_CHARTS['comp_detail'][pk] = comp_detail

    # Meta
    pesquisas = df.groupby(['Loja', 'Local', 'Data']).ngroups
    meta = {
        'ufs': sorted(df['Estado (UF)'].dropna().unique().tolist()),
        'lojas': sorted(df['Loja'].dropna().unique().tolist()),
        'marcas': sorted(df['Marca'].dropna().unique().tolist()),
        'd1': sorted(df['Departamento (Nível 1)'].dropna().unique().tolist()),
        'd2': sorted(df['Departamento (Nível 2)'].dropna().unique().tolist()),
        'd3': sorted(df['Departamento (Nível 3)'].dropna().unique().tolist()),
        'lojas_ambos': LOJAS_AMBOS,
        'total': len(df),
        'pesquisas': pesquisas,
        'pesquisas_fisico': int(df[df['canal'] == 'Físico'].groupby(['Loja', 'Local', 'Data']).ngroups),
        'pesquisas_online': int(df[df['canal'] == 'Online'].groupby(['Loja', 'Local', 'Data']).ngroups),
        'date_min': str(df['Data'].min())[:10],
        'date_max': str(df['Data'].max())[:10],
        'payment_labels': {
            'pix': 'PIX / À Vista', 'debito': 'Débito', 'boleto': 'Boleto',
            'cartao1x': 'Cartão (1x)', 'cartaoPrazo': 'Cartão Prazo s/ Juros',
            'cartaoPrazoJuros': 'Cartão Prazo c/ Juros', 'cartaoProprio': 'Cartão Próprio',
            'todos': 'Todos os Meios',
        }
    }

    return {
        'meta': meta,
        'prods_base': prods_base,
        'prods_stats': prods_stats,
        'lojas': lojas_data,
        'DTS': DTS, 'DUF': DUF, 'DDEPT': DDEPT,
        'DLOJA_COMP': DLOJA_COMP,
        'DLOJA_CHARTS': DLOJA_CHARTS,
    }, df

def inject_data_into_html(data, template_path='index.html'):
    """Injects new data into the dashboard HTML."""
    import re

    with open(template_path, 'r', encoding='utf-8') as f:
        html = f.read()

    def replace_js_const(html, name, value):
        js_value = json.dumps(value, ensure_ascii=False)
        pattern = rf'const {name} = (\{{|\[).*?(\}}|\]);'
        # Use a safe marker-based replacement
        marker = f'const {name} = '
        idx = html.find(marker)
        if idx == -1:
            print(f"  ⚠️  {name} not found in HTML")
            return html
        i = idx + len(marker)
        first = html[i]
        open_c = '[' if first == '[' else '{'
        close_c = ']' if first == '[' else '}'
        depth = 0; in_str = False; esc = False
        while i < len(html):
            c = html[i]
            if esc: esc = False
            elif c == '\\' and in_str: esc = True
            elif c == '"' and not in_str: in_str = True
            elif c == '"' and in_str: in_str = False
            elif not in_str:
                if c == open_c: depth += 1
                elif c == close_c:
                    depth -= 1
                    if depth == 0:
                        end = i + 1
                        if end < len(html) and html[end] == ';': end += 1
                        break
            i += 1
        html = html[:idx] + f'const {name} = {js_value};' + html[end:]
        print(f"  ✅ {name} updated")
        return html

    print("\n📝 Injecting data into HTML...")
    html = replace_js_const(html, 'DMETA', data['meta'])
    html = replace_js_const(html, 'DPRODS', data['prods_base'])
    html = replace_js_const(html, 'DPRODS_STATS', data['prods_stats'])
    html = replace_js_const(html, 'DLOJAS', data['lojas'])
    html = replace_js_const(html, 'DTS', data['DTS'])
    html = replace_js_const(html, 'DUF', data['DUF'])
    html = replace_js_const(html, 'DDEPT', data['DDEPT'])
    html = replace_js_const(html, 'DLOJA_COMP', data['DLOJA_COMP'])
    html = replace_js_const(html, 'DLOJA_CHARTS', data['DLOJA_CHARTS'])

    with open(template_path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"  💾 index.html updated ({len(html)/1024:.0f} KB)")

def register_upload(df, filename):
    """Registers upload in Supabase."""
    try:
        from supabase import create_client
        sb = create_client(SUPABASE_URL, SUPABASE_KEY)
        pesquisas_f = int(df[df['canal'] == 'Físico'].groupby(['Loja', 'Local', 'Data']).ngroups)
        pesquisas_o = int(df[df['canal'] == 'Online'].groupby(['Loja', 'Local', 'Data']).ngroups)
        sb.table('upload_history').insert({
            'filename': filename,
            'date_min': str(df['Data'].min())[:10],
            'date_max': str(df['Data'].max())[:10],
            'total_records': len(df),
            'pesquisas_fisico': pesquisas_f,
            'pesquisas_online': pesquisas_o,
            'status': 'active',
        }).execute()
        print("  ✅ Upload registrado no Supabase")
    except Exception as e:
        print(f"  ⚠️  Não foi possível registrar no Supabase: {e}")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Uso: python upload_base.py caminho/para/base.xlsx")
        sys.exit(1)

    filepath = sys.argv[1]
    if not Path(filepath).exists():
        print(f"❌ Arquivo não encontrado: {filepath}")
        sys.exit(1)

    print("=" * 50)
    print("  Dashboard Vivo — Processador de Base")
    print("=" * 50)

    data, df = process_xlsx(filepath)
    inject_data_into_html(data, 'index.html')
    register_upload(df, Path(filepath).name)

    print("\n" + "=" * 50)
    print("✅ Pronto! Próximos passos:")
    print("   1. git add index.html")
    print("   2. git commit -m 'Atualiza base de dados'")
    print("   3. git push")
    print("   O dashboard estará atualizado em ~1 minuto.")
    print("=" * 50)
