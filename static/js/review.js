// Image zoom controls
let zoomLevel = 1;

function zoomIn() {
  zoomLevel = Math.min(zoomLevel + 0.2, 3);
  applyZoom();
}

function zoomOut() {
  zoomLevel = Math.max(zoomLevel - 0.2, 0.4);
  applyZoom();
}

function resetZoom() {
  zoomLevel = 1;
  applyZoom();
}

function applyZoom() {
  const img = document.getElementById('docImage');
  if (img) img.style.transform = `scale(${zoomLevel})`;
}

// Keyboard navigation in properties table
document.addEventListener('keydown', function(e) {
  if (e.target.tagName !== 'INPUT') return;
  const row = e.target.closest('tr');
  if (!row || !row.closest('#propertiesTbody')) return;

  const inputs = Array.from(row.querySelectorAll('input'));
  const idx = inputs.indexOf(e.target);

  if (e.key === 'Tab' && !e.shiftKey && idx === inputs.length - 1) {
    e.preventDefault();
    const nextRow = row.nextElementSibling;
    if (nextRow) {
      nextRow.querySelector('input')?.focus();
    } else {
      // Add a new row at end of table
      if (typeof addRow === 'function') addRow();
    }
  }
});
