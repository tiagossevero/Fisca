"""
Módulo de Conexão com Banco de Dados
Gerencia conexões com Apache Impala e queries
"""

import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from typing import Optional, Dict, Any
import logging
from datetime import datetime

from src.config.settings import (
    IMPALA_HOST, IMPALA_PORT, DATABASE, RESOURCE_POOL,
    MENSAGENS
)

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """Gerenciador de conexões com Impala"""

    def __init__(self):
        self.host = IMPALA_HOST
        self.port = IMPALA_PORT
        self.database = DATABASE
        self.engine = None

    def get_connection_string(self, username: str = None, password: str = None) -> str:
        """Gera string de conexão"""
        if username and password:
            return f'impala://{username}:{password}@{self.host}:{self.port}/{self.database}'
        return f'impala://{self.host}:{self.port}/{self.database}'

    def connect(self, username: str = None, password: str = None) -> bool:
        """Estabelece conexão com o banco"""
        try:
            conn_string = self.get_connection_string(username, password)
            self.engine = create_engine(conn_string)
            return True
        except Exception as e:
            logger.error(f"Erro ao conectar: {e}")
            st.error(f"{MENSAGENS['erro_conexao']}: {str(e)}")
            return False

    def execute_query(self, query: str, params: Dict[str, Any] = None) -> Optional[pd.DataFrame]:
        """Executa query e retorna DataFrame"""
        try:
            if self.engine is None:
                if not self.connect():
                    return None

            # Adiciona configuração de resource pool
            full_query = f"SET REQUEST_POOL={RESOURCE_POOL};\n{query}"

            df = pd.read_sql(full_query, self.engine, params=params)
            return df
        except Exception as e:
            logger.error(f"Erro ao executar query: {e}")
            st.error(f"Erro na consulta: {str(e)}")
            return None

    def close(self):
        """Fecha conexão"""
        if self.engine:
            self.engine.dispose()


# Queries SQL Otimizadas
class Queries:
    """Biblioteca de queries SQL"""

    @staticmethod
    def get_dashboard_metrics(ano_inicio: int = None, ano_fim: int = None) -> str:
        """Query para métricas do dashboard"""
        filtro_ano = ""
        if ano_inicio and ano_fim:
            filtro_ano = f"WHERE ano BETWEEN {ano_inicio} AND {ano_fim}"

        return f"""
        SELECT
            COUNT(DISTINCT id_infracao) as total_infracoes,
            COUNT(DISTINCT cnpj) as total_empresas,
            COUNT(DISTINCT CASE WHEN notificacao_gerada = 1 THEN id_infracao END) as total_nfs,
            SUM(valor_infracao) as valor_total_infracoes,
            SUM(CASE WHEN notificacao_gerada = 1 THEN valor_infracao ELSE 0 END) as valor_total_nfs,
            AVG(CASE WHEN dias_ate_notificacao IS NOT NULL THEN dias_ate_notificacao END) as media_dias_notificacao,
            COUNT(DISTINCT afre_responsavel) as total_afres_ativos,
            MIN(data_infracao) as data_primeira_infracao,
            MAX(data_infracao) as data_ultima_infracao
        FROM fisca_fiscalizacoes_consolidadas
        {filtro_ano}
        """

    @staticmethod
    def get_temporal_evolution(group_by: str = 'mes') -> str:
        """Query para evolução temporal"""
        date_field = {
            'dia': 'data_infracao',
            'mes': "CONCAT(YEAR(data_infracao), '-', LPAD(MONTH(data_infracao), 2, '0'))",
            'ano': 'YEAR(data_infracao)'
        }.get(group_by, 'mes')

        return f"""
        SELECT
            {date_field} as periodo,
            COUNT(DISTINCT id_infracao) as qtd_infracoes,
            COUNT(DISTINCT CASE WHEN notificacao_gerada = 1 THEN id_infracao END) as qtd_nfs,
            COUNT(DISTINCT cnpj) as qtd_empresas,
            SUM(valor_infracao) as valor_total,
            AVG(CASE WHEN dias_ate_notificacao IS NOT NULL THEN dias_ate_notificacao END) as media_dias,
            COUNT(DISTINCT afre_responsavel) as qtd_afres
        FROM fisca_fiscalizacoes_consolidadas
        WHERE data_infracao IS NOT NULL
        GROUP BY {date_field}
        ORDER BY periodo
        """

    @staticmethod
    def get_performance_by_gerencia() -> str:
        """Query para performance por gerência"""
        return """
        SELECT
            gerencia,
            COUNT(DISTINCT id_infracao) as qtd_infracoes,
            COUNT(DISTINCT CASE WHEN notificacao_gerada = 1 THEN id_infracao END) as qtd_nfs,
            ROUND(COUNT(DISTINCT CASE WHEN notificacao_gerada = 1 THEN id_infracao END) * 100.0 /
                  NULLIF(COUNT(DISTINCT id_infracao), 0), 2) as taxa_conversao,
            SUM(valor_infracao) as valor_total,
            COUNT(DISTINCT cnpj) as qtd_empresas,
            COUNT(DISTINCT afre_responsavel) as qtd_afres,
            AVG(CASE WHEN dias_ate_notificacao IS NOT NULL THEN dias_ate_notificacao END) as media_dias_notificacao
        FROM fisca_fiscalizacoes_consolidadas
        WHERE gerencia IS NOT NULL
        GROUP BY gerencia
        ORDER BY valor_total DESC
        """

    @staticmethod
    def get_performance_by_cnae() -> str:
        """Query para performance por CNAE"""
        return """
        SELECT
            cnae_secao,
            cnae_divisao,
            cnae_descricao,
            COUNT(DISTINCT id_infracao) as qtd_infracoes,
            COUNT(DISTINCT CASE WHEN notificacao_gerada = 1 THEN id_infracao END) as qtd_nfs,
            ROUND(COUNT(DISTINCT CASE WHEN notificacao_gerada = 1 THEN id_infracao END) * 100.0 /
                  NULLIF(COUNT(DISTINCT id_infracao), 0), 2) as taxa_conversao,
            SUM(valor_infracao) as valor_total,
            COUNT(DISTINCT cnpj) as qtd_empresas
        FROM fisca_fiscalizacoes_consolidadas
        WHERE cnae_secao IS NOT NULL
        GROUP BY cnae_secao, cnae_divisao, cnae_descricao
        ORDER BY valor_total DESC
        """

    @staticmethod
    def get_geographic_analysis() -> str:
        """Query para análise geográfica"""
        return """
        SELECT
            municipio,
            uf,
            COUNT(DISTINCT id_infracao) as qtd_infracoes,
            COUNT(DISTINCT CASE WHEN notificacao_gerada = 1 THEN id_infracao END) as qtd_nfs,
            ROUND(COUNT(DISTINCT CASE WHEN notificacao_gerada = 1 THEN id_infracao END) * 100.0 /
                  NULLIF(COUNT(DISTINCT id_infracao), 0), 2) as taxa_conversao,
            SUM(valor_infracao) as valor_total,
            COUNT(DISTINCT cnpj) as qtd_empresas,
            AVG(valor_infracao) as valor_medio
        FROM fisca_fiscalizacoes_consolidadas
        WHERE municipio IS NOT NULL
        GROUP BY municipio, uf
        ORDER BY valor_total DESC
        """

    @staticmethod
    def get_afre_performance() -> str:
        """Query para performance de AFREs"""
        return """
        SELECT
            afre_responsavel,
            afre_nome,
            COUNT(DISTINCT id_infracao) as qtd_infracoes,
            COUNT(DISTINCT CASE WHEN notificacao_gerada = 1 THEN id_infracao END) as qtd_nfs,
            ROUND(COUNT(DISTINCT CASE WHEN notificacao_gerada = 1 THEN id_infracao END) * 100.0 /
                  NULLIF(COUNT(DISTINCT id_infracao), 0), 2) as taxa_conversao,
            SUM(valor_infracao) as valor_total,
            COUNT(DISTINCT cnpj) as qtd_empresas,
            AVG(CASE WHEN dias_ate_notificacao IS NOT NULL THEN dias_ate_notificacao END) as media_dias_notificacao,
            COUNT(DISTINCT DATE_FORMAT(data_infracao, '%Y-%m')) as meses_ativos,
            ROUND(COUNT(DISTINCT CASE WHEN notificacao_gerada = 1 THEN id_infracao END) * 1.0 /
                  NULLIF(COUNT(DISTINCT DATE_FORMAT(data_infracao, '%Y-%m')), 0), 2) as nfs_por_mes
        FROM fisca_fiscalizacoes_consolidadas
        WHERE afre_responsavel IS NOT NULL
        GROUP BY afre_responsavel, afre_nome
        ORDER BY qtd_nfs DESC
        """

    @staticmethod
    def get_infraction_types() -> str:
        """Query para tipos de infrações"""
        return """
        SELECT
            tipo_infracao,
            codigo_infracao,
            descricao_infracao,
            COUNT(DISTINCT id_infracao) as qtd_ocorrencias,
            COUNT(DISTINCT CASE WHEN notificacao_gerada = 1 THEN id_infracao END) as qtd_nfs,
            ROUND(COUNT(DISTINCT CASE WHEN notificacao_gerada = 1 THEN id_infracao END) * 100.0 /
                  NULLIF(COUNT(DISTINCT id_infracao), 0), 2) as taxa_conversao,
            SUM(valor_infracao) as valor_total,
            AVG(valor_infracao) as valor_medio,
            MIN(valor_infracao) as valor_minimo,
            MAX(valor_infracao) as valor_maximo
        FROM fisca_fiscalizacoes_consolidadas
        WHERE tipo_infracao IS NOT NULL
        GROUP BY tipo_infracao, codigo_infracao, descricao_infracao
        ORDER BY qtd_ocorrencias DESC
        """

    @staticmethod
    def get_company_details(cnpj: str) -> str:
        """Query para detalhes de empresa específica"""
        return f"""
        SELECT *
        FROM fisca_fiscalizacoes_consolidadas
        WHERE cnpj = '{cnpj}'
        ORDER BY data_infracao DESC
        """

    @staticmethod
    def get_ml_dataset() -> str:
        """Query para dataset de Machine Learning"""
        return """
        SELECT
            id_infracao,
            cnpj,
            razao_social,
            valor_infracao,
            YEAR(data_infracao) as ano_infracao,
            dias_ate_notificacao,
            tipo_infracao,
            cnae_secao,
            cnae_divisao,
            regime_tributario,
            municipio,
            gerencia,
            notificacao_gerada as target,
            CASE
                WHEN notificacao_gerada = 1 THEN 1
                ELSE 0
            END as converteu
        FROM fisca_fiscalizacoes_consolidadas
        WHERE valor_infracao > 0
            AND data_infracao IS NOT NULL
        ORDER BY data_infracao DESC
        LIMIT 50000
        """

    @staticmethod
    def get_network_data() -> str:
        """Query para análise de rede (empresas x AFREs x infrações)"""
        return """
        SELECT
            cnpj,
            razao_social,
            afre_responsavel,
            afre_nome,
            tipo_infracao,
            COUNT(*) as qtd_interacoes,
            SUM(valor_infracao) as valor_total
        FROM fisca_fiscalizacoes_consolidadas
        WHERE afre_responsavel IS NOT NULL
            AND tipo_infracao IS NOT NULL
        GROUP BY cnpj, razao_social, afre_responsavel, afre_nome, tipo_infracao
        HAVING qtd_interacoes >= 2
        ORDER BY qtd_interacoes DESC
        """


# Singleton para conexão global
@st.cache_resource
def get_database_connection() -> DatabaseConnection:
    """Retorna instância singleton da conexão"""
    return DatabaseConnection()
