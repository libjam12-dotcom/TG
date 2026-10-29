'use strict';
(() => {
  let loaded = false;
  function loadOwnerLink() {
    if (loaded || window.panelIdentity?.role !== 'owner') return;
    loaded = true;
    const script = document.createElement('script');
    script.src = '/community-link.js';
    document.head.append(script);
  }
  window.addEventListener('panel-identity', loadOwnerLink);
  loadOwnerLink();
})();
