# DataGuard UI Redesign

This build keeps `app.py` and the existing backend processing logic unchanged. The redesign is implemented in:

- `templates/index.html`
- `static/styles.css`
- `static/app.js`

## Main UI improvements

### Data Sources
- New guided ingestion layout.
- Selected files are shown before `Add Selected Source(s)` is clicked.
- Individual staged files can be removed before upload.
- Clear-all control for staged files.
- Drag-and-drop feedback.
- Connected sources are shown as a cleaner responsive source library.
- Current source preview remains available.

### Integration Studio
- Reorganized as a four-stage guided workflow: Name → Select → Connect → Build.
- Smart Integration is the default, prominent workflow.
- Source selection is clearer and base/right source selectors update immediately.
- Relationship analysis and auto-build are grouped together.
- Manual join controls are preserved inside an Advanced Manual Joins panel.
- Integration report, match quality, validation summary, issues and combined preview are preserved.

### Validation Studio
- Reorganized as Scan → Decide → Preview → Validate.
- Issue list is a responsive card board instead of a narrow internal scroll list.
- Affected-row preview initially shows the most relevant columns, with a `Show all columns` control to preserve full visibility.
- Fix options, preview, apply/revalidate, result tabs and advanced rules are all preserved.
- Advanced Validation Rules remain available without dominating the default workflow.

### Global UI
- New visual system with cleaner spacing, larger readable typography and consistent component hierarchy.
- Animated page entry, subtle moving gradients, hover motion and workflow progress states.
- Hidden browser scrollbar tracks in data tables; tables remain scrollable and support click/drag panning.
- Cleaner sidebar navigation and sticky top context bar.
- Responsive layouts for desktop, tablet and mobile.
- Updated chatbot presentation styling while preserving chatbot functionality.

## Compatibility

The redesign intentionally keeps the existing element IDs and API calls used by the JavaScript/backend so existing Flask features continue to connect to the same routes.
