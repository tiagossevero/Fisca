"""
Machine Learning
Sistema de priorização inteligente com múltiplos algoritmos
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score, roc_curve
import plotly.graph_objects as go
import plotly.express as px

from src.modules.database import get_database_connection
from src.modules.cache_manager import load_ml_dataset
from src.modules.charts import ChartBuilder
from src.utils.helpers import create_download_button
from src.config.settings import ML_TEST_SIZE_DEFAULT, ML_RANDOM_STATE, ML_MODELS


def render():
    """Renderiza página de Machine Learning"""
    st.title("🤖 Machine Learning - Priorização Inteligente")
    st.markdown("### Sistema de Predição de Conversão e Priorização de Casos")

    db = get_database_connection()
    chart_builder = ChartBuilder()

    # Configurações
    with st.sidebar:
        st.header("⚙️ Configurações do Modelo")

        algoritmo = st.selectbox(
            "Algoritmo",
            options=['random_forest', 'gradient_boosting', 'logistic_regression'],
            format_func=lambda x: ML_MODELS[x]['name']
        )

        test_size = st.slider("% Teste", 10, 40, int(ML_TEST_SIZE_DEFAULT * 100)) / 100

        min_valor = st.number_input("Valor Mínimo (R$)", value=1000.0, step=100.0)

        st.markdown("---")
        executar = st.button("🚀 Treinar Modelo", type="primary")

    # Carregar dados
    st.markdown("## 📊 Carregamento de Dados")

    with st.spinner("Carregando dataset..."):
        ml_df = load_ml_dataset(db)

    if ml_df is None or ml_df.empty:
        st.error("❌ Erro ao carregar dados")
        return

    # Filtrar por valor mínimo
    ml_df = ml_df[ml_df['valor_infracao'] >= min_valor]

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total de Registros", f"{len(ml_df):,}")

    with col2:
        taxa_conversao = (ml_df['target'].sum() / len(ml_df) * 100)
        st.metric("Taxa de Conversão", f"{taxa_conversao:.2f}%")

    with col3:
        st.metric("Casos Convertidos", f"{ml_df['target'].sum():,}")

    with col4:
        st.metric("Casos Não Convertidos", f"{(len(ml_df) - ml_df['target'].sum()):,}")

    if not executar:
        st.info("👆 Configure os parâmetros e clique em 'Treinar Modelo' para iniciar")
        return

    st.markdown("---")
    st.markdown("## 🔬 Preparação e Treinamento")

    # Feature Engineering
    with st.spinner("Preparando features..."):
        # Features numéricas
        ml_df['log_valor'] = np.log1p(ml_df['valor_infracao'])
        ml_df['dias_ate_notificacao'] = ml_df['dias_ate_notificacao'].fillna(0)

        # Encoding de categorias
        le_regime = LabelEncoder()
        le_tipo = LabelEncoder()
        le_cnae = LabelEncoder()

        ml_df['regime_encoded'] = le_regime.fit_transform(ml_df['regime_tributario'].fillna('Desconhecido'))
        ml_df['tipo_encoded'] = le_tipo.fit_transform(ml_df['tipo_infracao'].fillna('Outros'))
        ml_df['cnae_encoded'] = le_cnae.fit_transform(ml_df['cnae_secao'].fillna('Outros'))

        # Selecionar features
        feature_cols = ['log_valor', 'ano_infracao', 'dias_ate_notificacao',
                       'regime_encoded', 'tipo_encoded', 'cnae_encoded']

        X = ml_df[feature_cols].fillna(0)
        y = ml_df['target']

        # Split
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=ML_RANDOM_STATE, stratify=y
        )

        # Normalização
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)

    st.success(f"✅ Dataset preparado: {len(X_train)} treino, {len(X_test)} teste")

    # Treinamento
    st.markdown("### 🎯 Treinamento do Modelo")

    with st.spinner(f"Treinando {ML_MODELS[algoritmo]['name']}..."):
        # Selecionar modelo
        if algoritmo == 'random_forest':
            model = RandomForestClassifier(
                n_estimators=ML_MODELS[algoritmo]['n_estimators'],
                max_depth=ML_MODELS[algoritmo]['max_depth'],
                min_samples_split=ML_MODELS[algoritmo]['min_samples_split'],
                random_state=ML_RANDOM_STATE,
                n_jobs=-1
            )
        elif algoritmo == 'gradient_boosting':
            model = GradientBoostingClassifier(
                n_estimators=ML_MODELS[algoritmo]['n_estimators'],
                learning_rate=ML_MODELS[algoritmo]['learning_rate'],
                max_depth=ML_MODELS[algoritmo]['max_depth'],
                random_state=ML_RANDOM_STATE
            )
        else:
            model = LogisticRegression(
                max_iter=ML_MODELS[algoritmo]['max_iter'],
                C=ML_MODELS[algoritmo]['C'],
                random_state=ML_RANDOM_STATE
            )

        model.fit(X_train_scaled, y_train)

    st.success(f"✅ Modelo {ML_MODELS[algoritmo]['name']} treinado com sucesso!")

    # Predições
    y_pred = model.predict(X_test_scaled)
    y_proba = model.predict_proba(X_test_scaled)[:, 1]

    # Métricas
    st.markdown("## 📊 Avaliação do Modelo")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### 🎯 Matriz de Confusão")

        cm = confusion_matrix(y_test, y_pred)

        fig_cm = go.Figure(data=go.Heatmap(
            z=cm,
            x=['Não Converte', 'Converte'],
            y=['Não Converte', 'Converte'],
            colorscale='Blues',
            text=cm,
            texttemplate='%{text}',
            textfont={"size": 16},
            showscale=True
        ))

        fig_cm.update_layout(
            title="Matriz de Confusão",
            xaxis_title="Predito",
            yaxis_title="Real",
            height=400
        )

        st.plotly_chart(fig_cm, use_container_width=True)

    with col2:
        st.markdown("### 📈 Curva ROC")

        fpr, tpr, _ = roc_curve(y_test, y_proba)
        auc = roc_auc_score(y_test, y_proba)

        fig_roc = go.Figure()
        fig_roc.add_trace(go.Scatter(
            x=fpr, y=tpr,
            mode='lines',
            name=f'ROC (AUC = {auc:.3f})',
            line=dict(color='blue', width=3)
        ))
        fig_roc.add_trace(go.Scatter(
            x=[0, 1], y=[0, 1],
            mode='lines',
            name='Aleatório',
            line=dict(color='red', width=2, dash='dash')
        ))

        fig_roc.update_layout(
            title=f"Curva ROC - AUC: {auc:.3f}",
            xaxis_title="Taxa Falsos Positivos",
            yaxis_title="Taxa Verdadeiros Positivos",
            height=400
        )

        st.plotly_chart(fig_roc, use_container_width=True)

    # Report detalhado
    st.markdown("### 📋 Relatório de Classificação")

    report = classification_report(y_test, y_pred, output_dict=True)
    report_df = pd.DataFrame(report).transpose()

    st.dataframe(
        report_df.style.format({
            'precision': '{:.3f}',
            'recall': '{:.3f}',
            'f1-score': '{:.3f}',
            'support': '{:.0f}'
        }).background_gradient(subset=['f1-score'], cmap='RdYlGn'),
        use_container_width=True
    )

    # Feature Importance
    if hasattr(model, 'feature_importances_'):
        st.markdown("### 🔍 Importância das Features")

        importance_df = pd.DataFrame({
            'Feature': ['Log Valor', 'Ano', 'Dias NF', 'Regime', 'Tipo Infração', 'CNAE'],
            'Importance': model.feature_importances_
        }).sort_values('Importance', ascending=False)

        fig_importance = chart_builder.create_horizontal_bar_ranking(
            df=importance_df,
            category_col='Feature',
            value_col='Importance',
            title="Importância das Variáveis",
            top_n=6,
            color_scale=True
        )
        st.plotly_chart(fig_importance, use_container_width=True)

    # Priorização
    st.markdown("---")
    st.markdown("## 🎯 Priorização de Casos")

    # Predições em todo dataset
    X_full_scaled = scaler.transform(ml_df[feature_cols].fillna(0))
    ml_df['prob_conversao'] = model.predict_proba(X_full_scaled)[:, 1]

    # Score de prioridade
    valor_norm = (ml_df['valor_infracao'] - ml_df['valor_infracao'].min()) / \
                 (ml_df['valor_infracao'].max() - ml_df['valor_infracao'].min())

    ml_df['score_prioridade'] = (ml_df['prob_conversao'] * 0.6) + (valor_norm * 0.4)

    # Top casos prioritários
    top_priority = ml_df.nlargest(100, 'score_prioridade')[
        ['cnpj', 'razao_social', 'valor_infracao', 'tipo_infracao', 'prob_conversao', 'score_prioridade', 'target']
    ].copy()

    top_priority.columns = ['CNPJ', 'Razão Social', 'Valor', 'Tipo Infração', 'Prob. Conversão', 'Score', 'Converteu']

    st.markdown("### 🏆 Top 100 Casos Prioritários")

    st.dataframe(
        top_priority.style.format({
            'Valor': 'R$ {:,.2f}',
            'Prob. Conversão': '{:.2%}',
            'Score': '{:.3f}'
        }).background_gradient(subset=['Score'], cmap='RdYlGn'),
        use_container_width=True,
        height=400
    )

    # Distribuição de scores
    fig_dist = px.histogram(
        ml_df,
        x='score_prioridade',
        nbins=50,
        title="Distribuição de Scores de Prioridade",
        labels={'score_prioridade': 'Score de Prioridade', 'count': 'Frequência'}
    )
    st.plotly_chart(fig_dist, use_container_width=True)

    # Exportar
    st.markdown("### 📥 Exportar Resultados")

    col1, col2 = st.columns(2)

    with col1:
        create_download_button(
            top_priority,
            f"casos_prioritarios_{datetime.now().strftime('%Y%m%d')}.csv",
            "📊 Exportar Top 100 Prioritários"
        )

    with col2:
        export_full = ml_df[['cnpj', 'razao_social', 'valor_infracao', 'prob_conversao', 'score_prioridade']]
        create_download_button(
            export_full,
            f"predicoes_completas_{datetime.now().strftime('%Y%m%d')}.csv",
            "📊 Exportar Todas Predições"
        )
