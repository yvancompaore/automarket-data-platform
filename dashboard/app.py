"""
AutoMarket Intelligence Dashboard — mode LOCAL (sans S3)
Lit les Parquet depuis data/gold/ via DuckDB
"""

from pathlib import Path
import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

BASE_DIR = Path(__file__).parent.parent
GOLD_DIR = BASE_DIR / "data" / "gold"
DB_PATH  = GOLD_DIR / "automarket.duckdb"

st.set_page_config(
    page_title="AutoMarket Intelligence",
    page_icon="🚗",
    layout="wide",
)

# ─── Connexion DuckDB locale ──────────────────────────────────

@st.cache_resource
def get_db():
    if not DB_PATH.exists():
        return None
    return duckdb.connect(str(DB_PATH), read_only=True)


@st.cache_data(ttl=60)
def load(table: str) -> pd.DataFrame:
    con = get_db()
    if con is None:
        st.error("Base de données non trouvée. Lance d'abord : `python scripts/run_pipeline_local.py`")
        return pd.DataFrame()
    try:
        return con.execute(f"SELECT * FROM {table}").df()
    except Exception as e:
        st.warning(f"Table `{table}` non disponible : {e}")
        return pd.DataFrame()


# ─── Sidebar ─────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## 🚗 AutoMarket Intel")
    st.markdown("*Après-vente automobile*")
    if DB_PATH.exists():
        st.success("Base locale connectée")
    else:
        st.error("Base non trouvée")
    st.divider()
    page = st.radio(
        "Navigation",
        ["Vue d'ensemble", "Analyse Prix", "Couverture Catalogue", "Fournisseurs"],
    )
    st.divider()
    st.caption("Stack locale : MongoDB · Parquet · DuckDB · Streamlit")


# ─── Pages ───────────────────────────────────────────────────

if page == "Vue d'ensemble":
    st.title("Vue d'ensemble — Marché pièces auto")

    pricing_df   = load("mart_market_pricing")
    coverage_df  = load("mart_catalog_coverage")
    suppliers_df = load("mart_supplier_performance")

    if pricing_df.empty:
        st.stop()

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Références catalogue", f"{int(pricing_df['nb_catalog_references'].sum()):,}")
    with col2:
        st.metric("Score qualité moyen", f"{pricing_df['avg_quality_score'].mean():.0%}")
    with col3:
        st.metric("Annonces marketplace", f"{int(pricing_df['nb_market_listings'].sum()):,}")
    with col4:
        st.metric("Constructeurs couverts", len(coverage_df["vehicle_make"].unique()) if not coverage_df.empty else "—")

    st.divider()

    col_a, col_b = st.columns(2)
    with col_a:
        top = pricing_df.dropna(subset=["catalog_median_price"]).nlargest(8, "nb_catalog_references")
        fig = px.bar(
            top.sort_values("catalog_median_price"),
            x="catalog_median_price", y="category",
            orientation="h",
            color="catalog_median_price", color_continuous_scale="Blues",
            title="Prix médian par catégorie (catalogue)",
            labels={"catalog_median_price": "Prix médian (€)", "category": ""},
        )
        fig.update_layout(coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)

    with col_b:
        df_scatter = pricing_df.dropna(subset=["catalog_median_price", "market_median_price"])
        if not df_scatter.empty:
            fig = px.scatter(
                df_scatter,
                x="catalog_median_price", y="market_median_price",
                size="nb_catalog_references",
                color="market_vs_catalog_pct",
                color_continuous_scale="RdYlGn",
                hover_name="category",
                title="Catalogue vs Marché",
                labels={
                    "catalog_median_price": "Prix catalogue médian (€)",
                    "market_median_price": "Prix marché médian (€)",
                    "market_vs_catalog_pct": "Écart (%)",
                },
            )
            max_val = max(df_scatter["catalog_median_price"].max(), df_scatter["market_median_price"].max())
            fig.add_shape(type="line", x0=0, y0=0, x1=max_val, y1=max_val,
                          line=dict(color="gray", dash="dash"))
            st.plotly_chart(fig, use_container_width=True)


elif page == "Analyse Prix":
    st.title("Analyse des Prix")
    df = load("mart_market_pricing")
    if df.empty:
        st.stop()

    categories = ["Toutes"] + sorted(df["category"].dropna().unique().tolist())
    sel = st.selectbox("Catégorie", categories)
    if sel != "Toutes":
        df = df[df["category"] == sel]

    col1, col2 = st.columns(2)
    with col1:
        fig = px.bar(
            df.dropna(subset=["catalog_median_price"]).sort_values("catalog_median_price", ascending=False),
            x="category", y=["catalog_median_price", "market_median_price"],
            barmode="group",
            title="Catalogue vs Marché (prix médian)",
            labels={"value": "Prix (€)", "variable": "Source"},
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        if "market_liquidity" in df.columns:
            liq = df["market_liquidity"].value_counts().reset_index()
            liq.columns = ["Liquidité", "Count"]
            fig = px.pie(liq, values="Count", names="Liquidité",
                         title="Liquidité marché",
                         color_discrete_map={"haute": "#2DC653", "moyenne": "#F4A261", "faible": "#E63946"})
            st.plotly_chart(fig, use_container_width=True)

    cols = ["category", "brand", "nb_catalog_references", "catalog_median_price",
            "market_median_price", "market_vs_catalog_pct", "avg_quality_score"]
    st.dataframe(df[[c for c in cols if c in df.columns]], use_container_width=True)


elif page == "Couverture Catalogue":
    st.title("Couverture Catalogue — Par constructeur")
    df = load("mart_catalog_coverage")
    if df.empty:
        st.stop()

    col1, col2 = st.columns(2)
    with col1:
        top = df.nlargest(10, "total_parts")
        fig = px.bar(
            top, x="vehicle_make", y="coverage_score_pct",
            color="coverage_level" if "coverage_level" in df.columns else "coverage_score_pct",
            title="Score couverture par constructeur (%)",
        )
        fig.add_hline(y=80, line_dash="dash", line_color="green", annotation_text="Objectif 80%")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig = px.scatter(
            df, x="coverage_score_pct", y="total_parts",
            size="total_listings", color="vehicle_make",
            hover_name="vehicle_make",
            title="Couverture vs Volume pièces",
        )
        st.plotly_chart(fig, use_container_width=True)

    st.dataframe(df.nlargest(20, "total_parts"), use_container_width=True)


elif page == "Fournisseurs":
    st.title("Performance Fournisseurs")
    df = load("mart_supplier_performance")
    if df.empty:
        st.stop()

    col1, col2, col3 = st.columns(3)
    with col1:
        tier1 = len(df[df["supplier_tier"] == "Tier 1 — Stratégique"]) if "supplier_tier" in df.columns else 0
        st.metric("Fournisseurs Tier 1", tier1)
    with col2:
        st.metric("Qualité moyenne catalogue", f"{df['avg_quality_score'].mean():.0%}")
    with col3:
        st.metric("Dispo. moyenne", f"{df['availability_rate_pct'].mean():.1f}%")

    col_a, col_b = st.columns(2)
    with col_a:
        fig = px.bar(
            df.head(10).sort_values("supplier_score"),
            x="supplier_score", y="supplier",
            orientation="h",
            color="supplier_tier" if "supplier_tier" in df.columns else "supplier_score",
            title="Score fournisseurs (Top 10)",
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_b:
        fig = px.scatter(
            df,
            x="availability_rate_pct", y="avg_quality_score",
            size="nb_unique_refs",
            color="supplier_tier" if "supplier_tier" in df.columns else None,
            hover_name="supplier",
            title="Fiabilité vs Disponibilité",
        )
        fig.add_hline(y=0.7, line_dash="dash", line_color="orange")
        fig.add_vline(x=70,  line_dash="dash", line_color="orange")
        st.plotly_chart(fig, use_container_width=True)
