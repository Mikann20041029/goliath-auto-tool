(function(){
 const langKey="mk_lang";
 const supported=["ja","en","ko","zh"];
 function setLang(l){
 if(!supported.includes(l)) l="en";
 localStorage.setItem(langKey,l);
 document.documentElement.setAttribute("lang",l);
 document.querySelectorAll("[data-lang]").forEach(el=>{
 el.style.display = (el.getAttribute("data-lang")===l) ? "" : "none";
 });
 document.querySelectorAll("[data-set-lang]").forEach(b=>{
 b.classList.toggle("active", b.getAttribute("data-set-lang")===l);
 });
 }
 const initial = localStorage.getItem(langKey) || (navigator.language||"en").slice(0,2);
 setLang(supported.includes(initial)?initial:"en");
 document.addEventListener("click",(e)=>{
 const b=e.target.closest("[data-set-lang]");
 if(!b) return;
 setLang(b.getAttribute("data-set-lang"));
 });
})();