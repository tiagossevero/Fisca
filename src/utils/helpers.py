"""
Módulo de Utilitários e Funções Auxiliares
Funções de formatação, validação e processamento de dados
"""

import pandas as pd
import numpy as np
from typing import Optional, List, Dict, Any, Tuple
import re
from datetime import datetime, timedelta
import streamlit as st


class DataProcessor:
    """Processador de dados com funções utilitárias"""

    @staticmethod
    def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """Limpa e padroniza DataFrame"""
        if df is None or df.empty:
            return df

        # Converte nomes de colunas para minúsculas
        df.columns = df.columns.str.lower().str.strip()

        # Remove espaços em branco de colunas de texto
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].str.strip() if df[col].dtype == 'object' else df[col]

        return df

    @staticmethod
    def convert_numeric_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        """Converte colunas para numérico"""
        for col in columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        return df

    @staticmethod
    def calculate_percentile_ranges(series: pd.Series, n_bins: int = 5) -> pd.DataFrame:
        """Calcula faixas de percentis"""
        percentiles = np.linspace(0, 100, n_bins + 1)
        bins = np.percentile(series.dropna(), percentiles)
        labels = [f"P{int(percentiles[i])}-P{int(percentiles[i+1])}" for i in range(len(percentiles) - 1)]

        result = pd.cut(series, bins=bins, labels=labels, include_lowest=True)
        return pd.DataFrame({
            'value': series,
            'range': result
        })

    @staticmethod
    def aggregate_by_period(
        df: pd.DataFrame,
        date_col: str,
        agg_dict: Dict[str, str],
        period: str = 'M'
    ) -> pd.DataFrame:
        """Agrega dados por período (D=dia, W=semana, M=mês, Q=trimestre, Y=ano)"""
        df = df.copy()
        df[date_col] = pd.to_datetime(df[date_col])
        df.set_index(date_col, inplace=True)

        result = df.resample(period).agg(agg_dict).reset_index()
        return result

    @staticmethod
    def calculate_growth_rate(series: pd.Series, periods: int = 1) -> pd.Series:
        """Calcula taxa de crescimento"""
        return series.pct_change(periods=periods) * 100

    @staticmethod
    def calculate_moving_average(series: pd.Series, window: int = 3) -> pd.Series:
        """Calcula média móvel"""
        return series.rolling(window=window, min_periods=1).mean()

    @staticmethod
    def detect_outliers(series: pd.Series, method: str = 'iqr') -> pd.Series:
        """Detecta outliers usando IQR ou Z-score"""
        if method == 'iqr':
            Q1 = series.quantile(0.25)
            Q3 = series.quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            return (series < lower_bound) | (series > upper_bound)
        elif method == 'zscore':
            z_scores = np.abs((series - series.mean()) / series.std())
            return z_scores > 3
        else:
            return pd.Series([False] * len(series))

    @staticmethod
    def rank_with_ties(df: pd.DataFrame, col: str, ascending: bool = False) -> pd.DataFrame:
        """Cria ranking com tratamento de empates"""
        df = df.copy()
        df['rank'] = df[col].rank(ascending=ascending, method='min')
        return df.sort_values('rank')


class Formatter:
    """Formatador de dados para exibição"""

    @staticmethod
    def format_currency(value: float, decimals: int = 2) -> str:
        """Formata valor monetário em R$"""
        if pd.isna(value):
            return "R$ 0,00"
        return f"R$ {value:,.{decimals}f}".replace(",", "X").replace(".", ",").replace("X", ".")

    @staticmethod
    def format_percentage(value: float, decimals: int = 2) -> str:
        """Formata percentual"""
        if pd.isna(value):
            return "0,00%"
        return f"{value:.{decimals}f}%".replace(".", ",")

    @staticmethod
    def format_number(value: float, decimals: int = 0) -> str:
        """Formata número com separadores"""
        if pd.isna(value):
            return "0"
        return f"{value:,.{decimals}f}".replace(",", "X").replace(".", ",").replace("X", ".")

    @staticmethod
    def format_cnpj(cnpj: str) -> str:
        """Formata CNPJ"""
        cnpj = re.sub(r'\D', '', str(cnpj))
        if len(cnpj) != 14:
            return cnpj
        return f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:]}"

    @staticmethod
    def format_date(date: Any, format: str = '%d/%m/%Y') -> str:
        """Formata data"""
        if pd.isna(date):
            return ""
        if isinstance(date, str):
            date = pd.to_datetime(date)
        return date.strftime(format)

    @staticmethod
    def abbreviate_number(value: float) -> str:
        """Abrevia números grandes (K, M, B)"""
        if pd.isna(value):
            return "0"

        if abs(value) >= 1_000_000_000:
            return f"{value / 1_000_000_000:.2f}B"
        elif abs(value) >= 1_000_000:
            return f"{value / 1_000_000:.2f}M"
        elif abs(value) >= 1_000:
            return f"{value / 1_000:.2f}K"
        else:
            return f"{value:.2f}"


class Validator:
    """Validador de dados"""

    @staticmethod
    def validate_cnpj(cnpj: str) -> bool:
        """Valida CNPJ"""
        cnpj = re.sub(r'\D', '', str(cnpj))
        if len(cnpj) != 14:
            return False

        # Verifica se todos os dígitos são iguais
        if len(set(cnpj)) == 1:
            return False

        # Validação dos dígitos verificadores
        def calc_digit(cnpj: str, weights: List[int]) -> int:
            sum_val = sum(int(digit) * weight for digit, weight in zip(cnpj, weights))
            remainder = sum_val % 11
            return 0 if remainder < 2 else 11 - remainder

        weights_1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        weights_2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]

        digit_1 = calc_digit(cnpj[:12], weights_1)
        digit_2 = calc_digit(cnpj[:13], weights_2)

        return cnpj[-2:] == f"{digit_1}{digit_2}"

    @staticmethod
    def validate_date_range(start_date: datetime, end_date: datetime) -> bool:
        """Valida intervalo de datas"""
        return start_date <= end_date

    @staticmethod
    def validate_dataframe(df: pd.DataFrame, required_columns: List[str]) -> Tuple[bool, List[str]]:
        """Valida se DataFrame possui colunas obrigatórias"""
        missing = [col for col in required_columns if col not in df.columns]
        return len(missing) == 0, missing


class Calculator:
    """Calculadora de métricas de negócio"""

    @staticmethod
    def calculate_conversion_rate(converted: int, total: int) -> float:
        """Calcula taxa de conversão"""
        if total == 0:
            return 0.0
        return (converted / total) * 100

    @staticmethod
    def calculate_effectiveness_rate(
        converted: int,
        regularized: int,
        total_valid: int
    ) -> float:
        """Calcula taxa de efetividade (conversões + regularizações)"""
        if total_valid == 0:
            return 0.0
        return ((converted + regularized) / total_valid) * 100

    @staticmethod
    def calculate_average_ticket(total_value: float, count: int) -> float:
        """Calcula ticket médio"""
        if count == 0:
            return 0.0
        return total_value / count

    @staticmethod
    def calculate_concentration_index(df: pd.DataFrame, value_col: str, top_n: int = 5) -> float:
        """Calcula índice de concentração (% do top N sobre o total)"""
        total = df[value_col].sum()
        if total == 0:
            return 0.0

        top_sum = df.nlargest(top_n, value_col)[value_col].sum()
        return (top_sum / total) * 100

    @staticmethod
    def calculate_productivity(output: int, time_period: int) -> float:
        """Calcula produtividade (output por período)"""
        if time_period == 0:
            return 0.0
        return output / time_period

    @staticmethod
    def calculate_variance(actual: float, target: float) -> float:
        """Calcula variação percentual em relação à meta"""
        if target == 0:
            return 0.0
        return ((actual - target) / target) * 100


class AlertGenerator:
    """Gerador de alertas automáticos"""

    @staticmethod
    def check_conversion_rate(rate: float, threshold: float = 70.0) -> Dict[str, Any]:
        """Verifica taxa de conversão e gera alerta se necessário"""
        if rate < threshold:
            return {
                'type': 'warning',
                'title': '⚠️ Taxa de Conversão Abaixo da Meta',
                'message': f'Taxa atual: {rate:.2f}% | Meta: {threshold:.2f}%',
                'severity': 'high' if rate < threshold * 0.8 else 'medium'
            }
        else:
            return {
                'type': 'success',
                'title': '✅ Taxa de Conversão Atingida',
                'message': f'Taxa atual: {rate:.2f}% | Meta: {threshold:.2f}%',
                'severity': 'low'
            }

    @staticmethod
    def check_processing_time(avg_days: float, threshold: float = 60.0) -> Dict[str, Any]:
        """Verifica tempo médio de processamento"""
        if avg_days > threshold:
            return {
                'type': 'warning',
                'title': '⚠️ Tempo Médio Acima da Meta',
                'message': f'Tempo atual: {avg_days:.1f} dias | Meta: {threshold:.0f} dias',
                'severity': 'high' if avg_days > threshold * 1.5 else 'medium'
            }
        else:
            return {
                'type': 'success',
                'title': '✅ Tempo de Processamento Adequado',
                'message': f'Tempo atual: {avg_days:.1f} dias | Meta: {threshold:.0f} dias',
                'severity': 'low'
            }

    @staticmethod
    def check_productivity(nfs_per_month: float, threshold: float = 5.0) -> Dict[str, Any]:
        """Verifica produtividade"""
        if nfs_per_month < threshold:
            return {
                'type': 'warning',
                'title': '⚠️ Produtividade Abaixo da Meta',
                'message': f'NFs/mês: {nfs_per_month:.2f} | Meta: {threshold:.2f}',
                'severity': 'high' if nfs_per_month < threshold * 0.5 else 'medium'
            }
        else:
            return {
                'type': 'success',
                'title': '✅ Produtividade Adequada',
                'message': f'NFs/mês: {nfs_per_month:.2f} | Meta: {threshold:.2f}',
                'severity': 'low'
            }

    @staticmethod
    def detect_anomalies(df: pd.DataFrame, value_col: str, date_col: str) -> List[Dict[str, Any]]:
        """Detecta anomalias em série temporal"""
        alerts = []

        # Detecta outliers
        processor = DataProcessor()
        outliers = processor.detect_outliers(df[value_col])

        if outliers.any():
            outlier_dates = df[outliers][date_col].tolist()
            alerts.append({
                'type': 'info',
                'title': '📊 Anomalias Detectadas',
                'message': f'Encontradas {outliers.sum()} anomalias em: {", ".join(map(str, outlier_dates[:3]))}...',
                'severity': 'medium'
            })

        # Detecta tendência negativa
        if len(df) >= 3:
            recent_avg = df[value_col].tail(3).mean()
            previous_avg = df[value_col].head(len(df) - 3).mean()

            if recent_avg < previous_avg * 0.8:
                alerts.append({
                    'type': 'warning',
                    'title': '📉 Tendência de Queda Detectada',
                    'message': f'Redução de {((previous_avg - recent_avg) / previous_avg * 100):.1f}% em relação à média histórica',
                    'severity': 'high'
                })

        return alerts


class TableStyler:
    """Estilizador de tabelas para exibição"""

    @staticmethod
    def style_dataframe(
        df: pd.DataFrame,
        highlight_cols: List[str] = None,
        format_dict: Dict[str, str] = None
    ) -> pd.io.formats.style.Styler:
        """Aplica estilos a DataFrame para exibição"""
        styled = df.style

        # Aplica formatação
        if format_dict:
            styled = styled.format(format_dict)

        # Destaca colunas específicas
        if highlight_cols:
            styled = styled.set_properties(
                subset=highlight_cols,
                **{'background-color': '#e3f2fd', 'font-weight': 'bold'}
            )

        # Adiciona gradiente em colunas numéricas
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            if col in df.columns:
                styled = styled.background_gradient(
                    subset=[col],
                    cmap='Blues',
                    vmin=df[col].min(),
                    vmax=df[col].max()
                )

        return styled

    @staticmethod
    def create_html_table(df: pd.DataFrame, max_rows: int = 100) -> str:
        """Cria tabela HTML estilizada"""
        df_display = df.head(max_rows) if len(df) > max_rows else df

        html = '<div class="styled-table-container">'
        html += '<table class="styled-table">'

        # Cabeçalho
        html += '<thead><tr>'
        for col in df_display.columns:
            html += f'<th>{col}</th>'
        html += '</tr></thead>'

        # Corpo
        html += '<tbody>'
        for _, row in df_display.iterrows():
            html += '<tr>'
            for val in row:
                html += f'<td>{val}</td>'
            html += '</tr>'
        html += '</tbody>'

        html += '</table></div>'

        if len(df) > max_rows:
            html += f'<p><em>Mostrando {max_rows} de {len(df)} registros</em></p>'

        return html


def create_download_button(df: pd.DataFrame, filename: str, label: str = "📥 Baixar Dados"):
    """Cria botão de download para DataFrame"""
    csv = df.to_csv(index=False).encode('utf-8-sig')
    st.download_button(
        label=label,
        data=csv,
        file_name=filename,
        mime='text/csv'
    )


def show_alert(alert: Dict[str, Any]):
    """Exibe alerta formatado"""
    alert_type = alert.get('type', 'info')
    title = alert.get('title', '')
    message = alert.get('message', '')

    if alert_type == 'success':
        st.success(f"**{title}**\n\n{message}")
    elif alert_type == 'warning':
        st.warning(f"**{title}**\n\n{message}")
    elif alert_type == 'error':
        st.error(f"**{title}**\n\n{message}")
    else:
        st.info(f"**{title}**\n\n{message}")
