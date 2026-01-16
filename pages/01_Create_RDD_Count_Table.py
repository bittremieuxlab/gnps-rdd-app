# pages/01_Create_RDD_Count_Table.py
import os, sys, tempfile
import pandas as pd
import streamlit as st
import io

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from rdd import RDDCounts  # noqa: E402
from src.state_helpers import set_group  # noqa: E402


# ────────────────────── helpers ──────────────────────
def _read_any(upload):
    ext = os.path.splitext(upload.name)[1].lower()
    sep = "\t" if ext in (".tsv", ".txt") else ","
    return pd.read_csv(upload, sep=sep)


def _persist(upload):
    """Save UploadedFile -> temp file -> path"""
    suf = os.path.splitext(upload.name)[1]
    with tempfile.NamedTemporaryFile(delete=False, suffix=suf) as tmp:
        tmp.write(upload.getbuffer())
        return tmp.name


# ──────────────── Demo Data Helper ────────────────
def load_demo_file(filename):
    """Load a demo file as a BytesIO object with a name attribute."""
    path = os.path.join(ROOT, "data", filename)
    with open(path, "rb") as f:
        file_obj = io.BytesIO(f.read())
        file_obj.name = filename
        return file_obj


# ────────────────────── UI ──────────────────────
st.header("Create RDD Count Table")

# -------- METADATA EXPLANATION SECTION --------
with st.expander("ℹ️ What data do I need for RDD analysis?"):
    st.markdown(
        """
    ### Required Data
    
    **1. GNPS Network Data** (Required)
    - Your molecular networking results from GNPS
    - Can be uploaded as a file or fetched via GNPS Task ID
    
    **2. Reference Metadata** (Global foodomics project data preloaded by default; possible to add custom)
    - Links reference spectra to their known biological origin using ontology hierarchies
    - **Default:** Uses preloaded foodomics reference library with hierarchical food classifications
    - **Custom:** Upload your own reference metadata to use different ontology systems
    - **Columns needed:** `filename` (spectrum identifier) + sample_name + ontology columns (e.g., `kingdom`, `phylum`, `class`, etc.)
    
    **3. Sample Metadata** (Required for GNPS2, Optional for GNPS1/uploads)
    - Maps your sample files to experimental groups (e.g., treatment vs control)
    - **For GNPS2:** Always required to define sample groupings
    - **For GNPS1/File Upload:** Can use DefaultGroups from the network file if no custom metadata provided
    - **Columns needed:** `filename` (sample identifier) + `group` (or your chosen grouping column)
    
    """
    )

if "use_demo" not in st.session_state:
    st.session_state["use_demo"] = False

if st.button("Use Demo Data"):
    st.session_state["use_demo"] = True

use_demo = st.session_state["use_demo"]

# -------- input method selection --------
st.subheader("Data Input")
input_method = st.radio(
    "Choose input method:",
    ("Upload File", "GNPS Task ID"),
    disabled=use_demo,
    help="Select whether to upload a GNPS network file or fetch data directly from a GNPS job using its task ID",
)

gnps_file = None
gnps_task_id = None
gnps_version = None

if use_demo:
    gnps_file = load_demo_file("demo_gnps_network.tsv")
    sample_meta_up = load_demo_file("demo_gnps_metadata.csv")  # Load demo sample metadata
    ref_meta_up = load_demo_file("foodomics_multiproject_metadata.txt")
    st.success(
        "✅ Demo data loaded: GNPS network + sample metadata (Omnivore/Vegan groups) + reference metadata"
    )
    st.info("To use your own files, please reload the page.")
    # Pre-set demo groups for GNPS1 format demo data
    demo_sample_groups = ["G1", "G2"]
    demo_reference_groups = ["G4"]
elif input_method == "Upload File":
    gnps_file = st.file_uploader(
        "GNPS molecular network (.csv / .tsv)",
        type=("csv", "tsv"),
        help="Required: Your GNPS molecular networking output file",
    )
    sample_meta_up = st.file_uploader(
        "Sample metadata (GNPS2 requires; optional for GNPS1/file upload)",
        type=("csv", "tsv", "txt"),
        help="Optional: Maps filenames to experimental groups. If not provided, uses DefaultGroups from network file.",
    )
    ref_meta_up = st.file_uploader(
        "Reference metadata (uses preloaded foodomics data if not provided)",
        type=("csv", "tsv", "txt"),
        help="Hierarchical ontology annotations for reference spectra. Default foodomics metadata is used if not provided.",
    )
else:  # GNPS Task ID
    gnps_task_id = st.text_input(
        "Enter GNPS Task ID",
        placeholder="e.g., b93a540abded417ab1e2a285544a148c",
        help="Enter the task ID from your GNPS job URL",
    )

    st.info(
        "ℹ️ **Note:** Some GNPS jobs may not be accessible via Task ID due to server issues or archiving. "
        "If you encounter errors, please download the network file from your GNPS job page and use the 'Upload File' option instead."
    )
    gnps_version = st.radio(
        "GNPS Version:",
        ("GNPS2", "GNPS1 (Classic)"),
        horizontal=True,
        help="Select the GNPS version used for your analysis",
    )

    # Show warning for GNPS2 about required sample metadata
    if gnps_version == "GNPS2":
        st.warning(
            "⚠️ **GNPS2 requires sample metadata:** You must upload a sample metadata file with 'filename' and 'group' columns to define your experimental groups."
        )

    sample_meta_up = st.file_uploader(
        "Sample metadata (required for GNPS2, optional for GNPS1)",
        type=("csv", "tsv", "txt"),
        help="For GNPS2: REQUIRED to define sample groups. For GNPS1: Optional, uses DefaultGroups if not provided.",
    )
    ref_meta_up = st.file_uploader(
        "Reference metadata (uses preloaded foodomics data if not provided)",
        type=("csv", "tsv", "txt"),
        help="Hierarchical ontology annotations for reference spectra. Default foodomics metadata is used if not provided.",
    )

# -------- discover grouping options --------
sample_group_col = "group"
sample_groups_sel = []
reference_groups_sel = None

# Handle demo data groups
if use_demo:
    sample_groups_sel = demo_sample_groups
    reference_groups_sel = demo_reference_groups
    st.info(
        f"📊 Demo groups selected: Samples={sample_groups_sel}, References={reference_groups_sel}"
    )
elif sample_meta_up:
    meta_df = _read_any(sample_meta_up)
    sample_group_col = st.selectbox(
        "Column to group by",
        meta_df.columns,
        index=list(meta_df.columns).index("group") if "group" in meta_df.columns else 0,
    )
    sample_groups_sel = st.multiselect(
        "Sample groups to include",
        sorted(meta_df[sample_group_col].dropna().unique()),
        default=None,
        help="Leave blank to include all groups in the analysis",
    )
elif gnps_file:
    gnps_df = _read_any(gnps_file)
    if "DefaultGroups" in gnps_df.columns:
        sample_groups_sel = st.multiselect(
            "Sample groups to include",
            sorted(gnps_df["DefaultGroups"].dropna().unique()),
            default="G1",
            help="Leave blank to include all groups in the analysis",
        )
        reference_groups_sel = st.multiselect(
            "Reference groups to include",
            sorted(gnps_df["DefaultGroups"].dropna().unique()),
            default="G4",
            help="Leave blank to include all groups in the analysis",
        )
elif gnps_task_id and input_method == "GNPS Task ID":
    # For GNPS1, we need to fetch data to get available groups
    # For GNPS2, we can use sample metadata
    if gnps_version == "GNPS1 (Classic)":
        if gnps_task_id.strip():  # Only fetch if task_id is provided
            # Use session state to cache the fetched data (both groups and dataframe)
            cache_key_groups = f"gnps1_groups_{gnps_task_id}"
            cache_key_df = f"gnps1_df_{gnps_task_id}"

            if cache_key_groups not in st.session_state:
                # GNPS1 requires group selection from the network data
                with st.spinner("📊 Fetching GNPS1 data to display available groups..."):
                    try:
                        from rdd.utils import get_gnps_task_data

                        temp_gnps_df = get_gnps_task_data(gnps_task_id, gnps2=False)
                        if "DefaultGroups" in temp_gnps_df.columns:
                            available_groups = sorted(
                                temp_gnps_df["DefaultGroups"].dropna().unique()
                            )
                            st.session_state[cache_key_groups] = available_groups
                            # Cache the full dataframe for later use
                            st.session_state[cache_key_df] = temp_gnps_df
                            st.success("✅ Groups loaded successfully!")
                        else:
                            st.warning("Could not find DefaultGroups column in GNPS1 data.")
                            st.session_state[cache_key_groups] = []
                            st.session_state[cache_key_df] = None
                    except Exception as e:
                        error_msg = str(e)
                        st.error(f"❌ Failed to fetch GNPS data: {error_msg}")

                        # Check if it's an HTTP 500 error or similar server error
                        if "500" in error_msg or "HTTP" in error_msg.upper():
                            st.warning(
                                "⚠️ **Cannot Access GNPS Data via API**\n\n"
                                "This may occur due to archived jobs, server issues, or API problems.\n\n"
                                "**Alternative Solution:**\n"
                                "1. Visit your GNPS job page\n"
                                "2. Download the network file:\n"
                                "   - **GNPS1:** Look for `METABOLOMICS-SNETS-V2-[taskid]-view_all_clusters_withID_beta-main.tsv`\n"
                                "   - **GNPS2:** Look for `clusterinfo.tsv`\n"
                                "3. Return to this page and select 'Upload File' instead of 'GNPS Task ID'\n"
                                "4. Upload your downloaded network file"
                            )

                        st.session_state[cache_key_groups] = []
                        st.session_state[cache_key_df] = None

            # Use cached groups
            available_groups = st.session_state.get(cache_key_groups, [])
            if available_groups:
                sample_groups_sel = st.multiselect(
                    "Sample groups to include",
                    available_groups,
                    default=["G1"] if "G1" in available_groups else None,
                    help="Leave blank to include all groups in the analysis",
                )
                reference_groups_sel = st.multiselect(
                    "Reference groups to include",
                    available_groups,
                    default=["G4"] if "G4" in available_groups else None,
                    help="Leave blank to include all groups in the analysis",
                )
        else:
            st.info("👆 Please enter a GNPS Task ID above to load available groups.")
    else:
        # GNPS2 requires sample metadata to define groups
        if not sample_meta_up:
            st.error(
                "❌ GNPS2 requires sample metadata! Please upload a file with 'filename' and 'group' columns above."
            )
            st.stop()  # Prevent further execution without required metadata
        else:
            meta_df = _read_any(sample_meta_up)
            sample_group_col = st.selectbox(
                "Column to group by",
                meta_df.columns,
                index=(list(meta_df.columns).index("group") if "group" in meta_df.columns else 0),
            )
            sample_groups_sel = st.multiselect(
                "Sample groups to include",
                sorted(meta_df[sample_group_col].dropna().unique()),
                default=None,
                help="Leave blank to include all groups in the analysis",
            )

# -------- other parameters --------
sample_type = st.selectbox(
    "Reference sample type",
    ("all", "simple", "complex"),
    help="For foodomics data: 'simple' = single ingredient, 'complex' = multiple ingredients. For custom metadata, use 'all' unless it also contains this classification.",
)
st.info(
    "ℹ️ **Reference sample type:** This setting filters reference spectra based on the 'simple/complex' classification. "
    "**Foodomics data:** 'simple' = single ingredient foods, 'complex' = multi-ingredient foods. "
    "**Custom metadata:** Use 'all' unless your metadata includes this same classification."
)
ontology_cols = st.text_input("Custom ontology columns (comma-separated)", "")
levels_val = st.number_input(
    "Maximum ontology levels to analyse",
    0,
    10,
    None,
    1,
    help="Leave empty for automatic detection, or set to 0 for file-level counts only",
)

if (
    ontology_cols
    and levels_val
    and levels_val > len([c for c in ontology_cols.split(",") if c.strip()])
):
    st.warning("Reducing 'levels' to match number of ontology columns.")
    levels_val = len([c for c in ontology_cols.split(",") if c.strip()])

# -------- run --------
if st.button("Generate RDD Counts"):
    # Validate inputs based on method
    if input_method == "Upload File" and not gnps_file:
        st.error("GNPS file required.")
        st.stop()
    elif input_method == "GNPS Task ID" and not gnps_task_id:
        st.error("GNPS Task ID required.")
        st.stop()

    # Prepare paths for uploaded files
    gnps_path = _persist(gnps_file) if gnps_file else None
    sample_meta_p = _persist(sample_meta_up) if sample_meta_up else None
    ref_meta_p = _persist(ref_meta_up) if ref_meta_up else None
    ontology_list = [c.strip() for c in ontology_cols.split(",") if c.strip()]

    try:
        # Determine whether to use task_id or file path
        if gnps_task_id:
            # Check if we have cached GNPS data (for GNPS1)
            cache_key_df = f"gnps1_df_{gnps_task_id}"
            if cache_key_df in st.session_state and st.session_state[cache_key_df] is not None:
                # Use cached dataframe - save it to a temp file and use file path instead
                with st.spinner("Using cached GNPS data..."):
                    cached_df = st.session_state[cache_key_df]
                    with tempfile.NamedTemporaryFile(delete=False, suffix=".tsv", mode="w") as tmp:
                        cached_df.to_csv(tmp.name, sep="\t", index=False)
                        gnps_path = tmp.name

                    rdd = RDDCounts(
                        gnps_network_path=gnps_path,
                        sample_types=sample_type,
                        sample_groups=sample_groups_sel or None,
                        sample_group_col=sample_group_col,
                        levels=levels_val,
                        external_reference_metadata=ref_meta_p,
                        external_sample_metadata=sample_meta_p,
                        ontology_columns=ontology_list or None,
                        reference_groups=reference_groups_sel or None,
                    )
            else:
                # No cached data - fetch via task_id (for GNPS2 or if cache missing)
                with st.spinner(
                    f"🔄 Fetching GNPS{' 2' if gnps_version == 'GNPS2' else '1'} data from task {gnps_task_id}..."
                ):
                    rdd = RDDCounts(
                        task_id=gnps_task_id,
                        gnps_2=(gnps_version == "GNPS2"),
                        sample_types=sample_type,
                        sample_groups=sample_groups_sel or None,
                        sample_group_col=sample_group_col,
                        levels=levels_val,
                        external_reference_metadata=ref_meta_p,
                        external_sample_metadata=sample_meta_p,
                        ontology_columns=ontology_list or None,
                        reference_groups=reference_groups_sel or None,
                    )
        else:
            rdd = RDDCounts(
                gnps_network_path=gnps_path,
                sample_types=sample_type,
                sample_groups=sample_groups_sel or None,
                sample_group_col=sample_group_col,
                levels=levels_val,
                external_reference_metadata=ref_meta_p,
                external_sample_metadata=sample_meta_p,
                ontology_columns=ontology_list or None,
                reference_groups=reference_groups_sel or None,
            )

        # make chosen column the live group
        set_group(rdd, sample_group_col)

        st.session_state["rdd"] = rdd
        st.success("✅ RDDCounts object created successfully!")

    except Exception as e:
        error_msg = str(e)
        st.error(f"❌ Error creating RDDCounts: {error_msg}")

        # Provide specific guidance for HTTP errors with task IDs
        if gnps_task_id and (
            "500" in error_msg or "HTTP" in error_msg.upper() or "404" in error_msg
        ):
            st.warning(
                "⚠️ **Cannot Access GNPS Job Data**\n\n"
                "This may occur due to server issues, archived jobs, or temporary API problems.\n\n"
                "**Recommended Solution:**\n\n"
                "1. Go to your GNPS job page in your browser\n"
                "2. Download the network file:\n"
                "   - **GNPS1:** `METABOLOMICS-SNETS-V2-[taskid]-view_all_clusters_withID_beta-main.tsv`\n"
                "   - **GNPS2:** `clusterinfo.tsv`\n"
                "3. Change input method above to **'Upload File'**\n"
                "4. Upload your downloaded file\n\n"
                "This will bypass the API and work with any GNPS job."
            )
        else:
            st.exception(e)


# -------- GROUP ASSIGNMENT SECTION (OUTSIDE BUTTON BLOCK) --------
# This section allows updating group assignments and persists across reruns
if "rdd" in st.session_state:
    rdd = st.session_state["rdd"]

    st.markdown("---")
    st.markdown("### 🏷️ Update Group Assignments")

    # Check if this is demo data
    if use_demo and "demo_groups_applied" not in st.session_state:
        st.info(
            "🎯 **Demo Mode**: Click the button below to apply meaningful group names (Omnivore/Vegan) to your samples."
        )

        # Provide download button for demo mapping file as an example
        demo_mapping_example = load_demo_file("demo_gnps_metadata.csv")
        mapping_example_df = pd.read_csv(demo_mapping_example)
        mapping_example_df["filename"] = mapping_example_df["filename"].str.replace(".mzXML", "")
        mapping_example_df["group"] = mapping_example_df["group"].str.replace("G1", "Omnivore")
        mapping_example_df["group"] = mapping_example_df["group"].str.replace("G2", "Vegan")

        # Rename to match the expected format for custom mapping
        mapping_example_for_download = mapping_example_df.rename(columns={"group": "new_group"})

        col1, col2 = st.columns([1, 1])
        with col1:
            if st.button(
                "🔄 Apply Demo Group Names (G1→Omnivore, G2→Vegan)", key="apply_demo_groups"
            ):
                demo_mapping = load_demo_file("demo_gnps_metadata.csv")
                mapping_df = pd.read_csv(demo_mapping)
                mapping_df["filename"] = mapping_df["filename"].str.replace(".mzXML", "")
                mapping_df["group"] = mapping_df["group"].str.replace("G1", "Omnivore")
                mapping_df["group"] = mapping_df["group"].str.replace("G2", "Vegan")

                if {"filename", "group"}.issubset(mapping_df.columns):
                    rdd.counts = rdd.counts.drop("group", axis=1, errors="ignore").merge(
                        mapping_df[["filename", "group"]],
                        left_on="filename",
                        right_on="filename",
                        how="left",
                    )
                    # Update sample_metadata with new group assignments
                    if "group" in rdd.counts.columns and "filename" in rdd.counts.columns:
                        rdd.sample_metadata = rdd.sample_metadata.drop(
                            "group", axis=1, errors="ignore"
                        ).merge(
                            rdd.counts[["filename", "group"]].drop_duplicates(),
                            on="filename",
                            how="left",
                        )

                        # Also update the original sample_group_col if it's different from "group"
                        if (
                            rdd.sample_group_col != "group"
                            and rdd.sample_group_col in rdd.sample_metadata.columns
                        ):
                            rdd.sample_metadata[rdd.sample_group_col] = rdd.sample_metadata["group"]

                    st.session_state["rdd"] = rdd
                    st.session_state["demo_groups_applied"] = True
                    st.session_state["custom_mapping_applied"] = (
                        True  # Mark as custom mapping applied
                    )
                    st.success(
                        "✅ Demo group assignments applied! Groups updated: G1 → Omnivore, G2 → Vegan"
                    )
                    st.dataframe(rdd.counts.sample(15))
                    st.rerun()
                else:
                    st.warning("Demo sample metadata must have columns: filename, group")

        with col2:
            # Download button for example mapping file
            mapping_csv = mapping_example_for_download.to_csv(index=False)
            st.download_button(
                label="📥 Download Example Mapping File",
                data=mapping_csv,
                file_name="demo_group_mapping_example.csv",
                mime="text/csv",
                help="Download this example mapping file to see the required format (filename, new_group)",
                key="download_demo_mapping",
            )

        # Show preview of what the mapping looks like
        with st.expander("👁️ Preview Demo Mapping File Format"):
            st.caption("This shows what a group mapping file should look like:")
            st.dataframe(mapping_example_for_download.head(10))
            st.info("**Required columns:** `filename` and `new_group`")

    elif use_demo and "demo_groups_applied" in st.session_state:
        st.success("✅ Demo groups already applied (Omnivore/Vegan)")

    # --- Allow user to upload a mapping file to change group assignments ---
    st.markdown("#### Upload Custom Group Mapping")
    st.caption(
        "Upload a CSV/TSV file with 'filename' and 'new_group' columns to reassign samples to different groups."
    )

    mapping_file = st.file_uploader(
        "Upload a mapping file (CSV/TSV: filename,new_group)",
        type=["csv", "tsv"],
        key="mapping",
        help="File should contain 'filename' column matching your sample names and 'new_group' column with new group assignments",
    )

    if mapping_file:
        ext = os.path.splitext(mapping_file.name)[1].lower()
        sep = "\t" if ext in (".tsv", ".txt") else ","
        mapping_df = pd.read_csv(mapping_file, sep=sep)

        # Strip file extensions from filenames to match RDD data format
        mapping_df["filename"] = mapping_df["filename"].str.replace(
            r"\.(mzML|mzXML|mgf|mzml|mzxml)$", "", regex=True
        )

        # Preview the mapping file
        st.markdown("**Preview of mapping file (after removing extensions):**")
        st.dataframe(mapping_df.head())

        if st.button("🔄 Apply Custom Group Mapping", key="apply_custom_mapping"):
            if {"filename", "new_group"}.issubset(mapping_df.columns):
                # Save the mapping file temporarily to use with update_groups
                with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
                    mapping_df.to_csv(tmp.name, index=False)
                    tmp_path = tmp.name

                try:
                    # Use the built-in update_groups method - just updates labels, doesn't recalculate
                    rdd.update_groups(tmp_path, merge_column="new_group")

                    # Ensure the visualization 'group' column is synced
                    set_group(rdd, rdd.sample_group_col)

                    st.session_state["rdd"] = rdd
                    st.session_state["custom_mapping_applied"] = True
                    st.success("✅ Custom group assignments applied!")

                    # Show updated counts to verify the change
                    st.markdown("**Updated counts (first 15 rows):**")
                    st.dataframe(
                        rdd.counts[["filename", "reference_type", "count", "level", "group"]].head(
                            15
                        )
                    )
                finally:
                    # Clean up temp file
                    if os.path.exists(tmp_path):
                        os.unlink(tmp_path)
                st.rerun()
            else:
                st.error("❌ Mapping file must have columns: filename, new_group")

# -------- DISPLAY LOADED METADATA (OUTSIDE BUTTON BLOCK) --------
# This section persists across page reruns when RDD is in session_state
if "rdd" in st.session_state:
    rdd = st.session_state["rdd"]

    st.markdown("---")
    st.markdown("### 📊 Loaded Data Summary")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Reference Metadata**")
        st.caption(
            f"Shape: {rdd.reference_metadata.shape[0]} reference spectra × {rdd.reference_metadata.shape[1]} columns"
        )
        with st.expander("View reference metadata (first 10 rows)"):
            st.dataframe(rdd.reference_metadata.head(10))

        # Download button for reference metadata
        ref_csv = rdd.reference_metadata.to_csv(index=False)
        st.download_button(
            label="📥 Download Reference Metadata",
            data=ref_csv,
            file_name="reference_metadata.csv",
            mime="text/csv",
            help="Download the complete reference metadata as CSV",
            key="download_ref_meta",
        )

        # Show ontology structure
        ontology_info = f"**Ontology levels:** {rdd.levels}"
        if rdd.ontology_columns_renamed:
            ontology_info += (
                f"\n\n**Custom ontology columns:** {', '.join(rdd.ontology_columns_renamed)}"
            )
        else:
            ontology_cols = [f"sample_type_group{i}" for i in range(1, rdd.levels + 1)]
            ontology_info += f"\n\n**Default ontology columns:** {', '.join(ontology_cols)}"
        st.info(ontology_info)

    with col2:
        st.markdown("**Sample Metadata**")
        st.caption(
            f"Shape: {rdd.sample_metadata.shape[0]} samples × {rdd.sample_metadata.shape[1]} columns"
        )
        with st.expander("View sample metadata (first 10 rows)"):
            st.dataframe(rdd.sample_metadata.head(10))

        # Download button for sample metadata
        sample_csv = rdd.sample_metadata.to_csv(index=False)
        st.download_button(
            label="📥 Download Sample Metadata",
            data=sample_csv,
            file_name="sample_metadata.csv",
            mime="text/csv",
            help="Download the complete sample metadata as CSV",
            key="download_sample_meta",
        )

        # Show grouping information
        if "group" in rdd.sample_metadata.columns:
            groups = rdd.sample_metadata["group"].value_counts()
            group_info = f"**Grouping column:** `group`\n\n"
            group_info += "**Groups:**\n"
            for grp, cnt in groups.items():
                group_info += f"- {grp}: {cnt} samples\n"
            st.info(group_info)

    st.markdown("---")
    st.markdown("### 🔢 RDD Count Table Preview")
    st.dataframe(rdd.counts.head(15))

    # Download button for RDD counts
    counts_csv = rdd.counts.to_csv(index=False)
    st.download_button(
        label="📥 Download Complete RDD Counts Table",
        data=counts_csv,
        file_name="rdd_counts_table.csv",
        mime="text/csv",
        help="Download the complete RDD counts table as CSV",
        key="download_rdd_counts",
    )
