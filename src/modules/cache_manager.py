"""
Módulo de Gerenciamento de Cache
Sistema avançado de cache com TTL e invalidação
"""

import streamlit as st
import pandas as pd
from typing import Optional, Callable, Any
from datetime import datetime, timedelta
import hashlib
import json
import logging

from src.config.settings import CACHE_TTL_GLOBAL, CACHE_TTL_CONSULTAS, CACHE_TTL_ML

logger = logging.getLogger(__name__)


class CacheManager:
    """Gerenciador avançado de cache"""

    @staticmethod
    def get_cache_key(prefix: str, **kwargs) -> str:
        """Gera chave única para cache baseada em parâmetros"""
        params_str = json.dumps(kwargs, sort_keys=True)
        hash_obj = hashlib.md5(params_str.encode())
        return f"{prefix}_{hash_obj.hexdigest()}"

    @staticmethod
    def cache_data(ttl: int = CACHE_TTL_GLOBAL):
        """Decorator para cache de dados com TTL customizado"""
        return st.cache_data(ttl=ttl, show_spinner=True)

    @staticmethod
    def cache_resource(ttl: int = CACHE_TTL_GLOBAL):
        """Decorator para cache de recursos com TTL customizado"""
        return st.cache_resource(ttl=ttl, show_spinner=True)


# Funções de cache específicas
@st.cache_data(ttl=CACHE_TTL_GLOBAL, show_spinner="📊 Carregando métricas do dashboard...")
def load_dashboard_metrics(db, ano_inicio: int = None, ano_fim: int = None) -> Optional[pd.DataFrame]:
    """Carrega e cacheia métricas do dashboard"""
    from src.modules.database import Queries
    query = Queries.get_dashboard_metrics(ano_inicio, ano_fim)
    return db.execute_query(query)


@st.cache_data(ttl=CACHE_TTL_GLOBAL, show_spinner="📈 Carregando evolução temporal...")
def load_temporal_evolution(db, group_by: str = 'mes') -> Optional[pd.DataFrame]:
    """Carrega e cacheia evolução temporal"""
    from src.modules.database import Queries
    query = Queries.get_temporal_evolution(group_by)
    return db.execute_query(query)


@st.cache_data(ttl=CACHE_TTL_GLOBAL, show_spinner="🏢 Carregando dados por gerência...")
def load_performance_by_gerencia(db) -> Optional[pd.DataFrame]:
    """Carrega e cacheia performance por gerência"""
    from src.modules.database import Queries
    query = Queries.get_performance_by_gerencia()
    return db.execute_query(query)


@st.cache_data(ttl=CACHE_TTL_GLOBAL, show_spinner="🏭 Carregando dados por CNAE...")
def load_performance_by_cnae(db) -> Optional[pd.DataFrame]:
    """Carrega e cacheia performance por CNAE"""
    from src.modules.database import Queries
    query = Queries.get_performance_by_cnae()
    return db.execute_query(query)


@st.cache_data(ttl=CACHE_TTL_GLOBAL, show_spinner="🗺️ Carregando análise geográfica...")
def load_geographic_analysis(db) -> Optional[pd.DataFrame]:
    """Carrega e cacheia análise geográfica"""
    from src.modules.database import Queries
    query = Queries.get_geographic_analysis()
    return db.execute_query(query)


@st.cache_data(ttl=CACHE_TTL_GLOBAL, show_spinner="👥 Carregando performance dos AFREs...")
def load_afre_performance(db) -> Optional[pd.DataFrame]:
    """Carrega e cacheia performance de AFREs"""
    from src.modules.database import Queries
    query = Queries.get_afre_performance()
    return db.execute_query(query)


@st.cache_data(ttl=CACHE_TTL_GLOBAL, show_spinner="⚖️ Carregando tipos de infrações...")
def load_infraction_types(db) -> Optional[pd.DataFrame]:
    """Carrega e cacheia tipos de infrações"""
    from src.modules.database import Queries
    query = Queries.get_infraction_types()
    return db.execute_query(query)


@st.cache_data(ttl=CACHE_TTL_CONSULTAS, show_spinner="🔍 Buscando dados da empresa...")
def load_company_details(db, cnpj: str) -> Optional[pd.DataFrame]:
    """Carrega e cacheia detalhes de empresa específica"""
    from src.modules.database import Queries
    query = Queries.get_company_details(cnpj)
    return db.execute_query(query)


@st.cache_data(ttl=CACHE_TTL_ML, show_spinner="🤖 Preparando dataset para ML...")
def load_ml_dataset(db) -> Optional[pd.DataFrame]:
    """Carrega e cacheia dataset para Machine Learning"""
    from src.modules.database import Queries
    query = Queries.get_ml_dataset()
    df = db.execute_query(query)

    if df is not None and not df.empty:
        # Processamento básico
        df = df.dropna(subset=['valor_infracao', 'ano_infracao'])
        df['valor_infracao'] = pd.to_numeric(df['valor_infracao'], errors='coerce')
        df = df[df['valor_infracao'] > 0]

    return df


@st.cache_data(ttl=CACHE_TTL_GLOBAL, show_spinner="🕸️ Carregando dados de rede...")
def load_network_data(db) -> Optional[pd.DataFrame]:
    """Carrega e cacheia dados para análise de rede"""
    from src.modules.database import Queries
    query = Queries.get_network_data()
    return db.execute_query(query)


class DataCache:
    """Classe para gerenciar cache de dados processados"""

    def __init__(self):
        if 'data_cache' not in st.session_state:
            st.session_state.data_cache = {}
            st.session_state.cache_timestamps = {}

    def set(self, key: str, value: Any, ttl: int = CACHE_TTL_GLOBAL):
        """Armazena valor no cache com TTL"""
        st.session_state.data_cache[key] = value
        st.session_state.cache_timestamps[key] = datetime.now() + timedelta(seconds=ttl)

    def get(self, key: str) -> Optional[Any]:
        """Recupera valor do cache se ainda válido"""
        if key not in st.session_state.data_cache:
            return None

        # Verifica se expirou
        if datetime.now() > st.session_state.cache_timestamps.get(key, datetime.now()):
            self.invalidate(key)
            return None

        return st.session_state.data_cache[key]

    def invalidate(self, key: str):
        """Invalida entrada específica do cache"""
        if key in st.session_state.data_cache:
            del st.session_state.data_cache[key]
        if key in st.session_state.cache_timestamps:
            del st.session_state.cache_timestamps[key]

    def clear_all(self):
        """Limpa todo o cache"""
        st.session_state.data_cache = {}
        st.session_state.cache_timestamps = {}

    def get_stats(self) -> dict:
        """Retorna estatísticas do cache"""
        total_items = len(st.session_state.data_cache)
        valid_items = sum(
            1 for key in st.session_state.data_cache
            if datetime.now() <= st.session_state.cache_timestamps.get(key, datetime.now())
        )
        expired_items = total_items - valid_items

        return {
            'total_items': total_items,
            'valid_items': valid_items,
            'expired_items': expired_items
        }


def clear_all_caches():
    """Limpa todos os caches do Streamlit e session state"""
    st.cache_data.clear()
    st.cache_resource.clear()
    DataCache().clear_all()
    st.success("🔄 Todos os caches foram limpos!")


def get_cache_stats() -> str:
    """Retorna estatísticas de cache formatadas"""
    cache = DataCache()
    stats = cache.get_stats()

    return f"""
    📦 **Estatísticas de Cache:**
    - Total de itens: {stats['total_items']}
    - Itens válidos: {stats['valid_items']}
    - Itens expirados: {stats['expired_items']}
    """
