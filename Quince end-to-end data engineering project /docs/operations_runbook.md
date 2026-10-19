# Make the current CSV pipeline rerun-safe

For our existing file-based sources, start with deterministic destination paths and a manifest check.

**A. Use a stable batch identifier**
**B. Check the manifest before copying**
**C. Use a deterministic output path**

Record success and failure correctly
After the Copy activity, connect separate success and failure logging paths.

**On success:**
1. Insert or update the manifest entry to Succeeded.
2. Record the pipeline run ID and rows copied.
3. Update the audit table.
4. Allow the batch to complete.

**On failure:**
1. Record the manifest status as Failed.
2. Capture the error message and pipeline run ID.
3. Write the failed attempt to pipeline_audit.
4. Do not mark the batch as succeeded.

Implement database-style watermarks only when the source has a suitable incremental column or change-tracking mechanism.
For CSV sources, use the file manifest first. So (DB source ->watermarking, CSV source ->file_manifest)
