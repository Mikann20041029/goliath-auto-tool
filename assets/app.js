(function(){
  const LANG_KEY="mk_lang";
  const THEME_KEY="mk_theme";
  const supported=["en","ja","ko","zh"];

  function applyLang(l){
    if(!supported.includes(l)) l="en";
    localStorage.setItem(LANG_KEY,l);
    document.documentElement.setAttribute("lang", l);
    document.querySelectorAll("[data-lang]").forEach(el=>{
      el.style.display = (el.getAttribute("data-lang")===l) ? "" : "none";
    });
    document.querySelectorAll("[data-set-lang]").forEach(b=>{
      b.classList.toggle("active", b.getAttribute("data-set-lang")===l);
    });
  }

  function applyTheme(t){
    if(t!=="light" && t!=="dark") t="light";
    localStorage.setItem(THEME_KEY, t);
    document.documentElement.setAttribute("data-theme", t);
    const btn = document.querySelector("[data-toggle-theme]");
    if(btn){
      btn.textContent = (t==="dark") ? "Dark" : "Light";
      btn.setAttribute("aria-pressed", t==="dark" ? "true" : "false");
    }
  }

  // Default: English
  const initialLang = localStorage.getItem(LANG_KEY) || "en";
  applyLang(initialLang);

  // Theme default: follow system if never set
  const savedTheme = localStorage.getItem(THEME_KEY);
  if(savedTheme){
    applyTheme(savedTheme);
  }else{
    const sysDark = window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches;
    applyTheme(sysDark ? "dark" : "light");
  }

  document.addEventListener("click",(e)=>{
    const lb=e.target.closest("[data-set-lang]");
    if(lb){ applyLang(lb.getAttribute("data-set-lang")); return; }

    const tb=e.target.closest("[data-toggle-theme]");
    if(tb){
      const cur = document.documentElement.getAttribute("data-theme") || "light";
      applyTheme(cur==="dark" ? "light" : "dark");
      return;
    }
  });
})();