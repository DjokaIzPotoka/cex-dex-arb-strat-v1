# Long-Tail MEV Opportunity Database — Faza 1: Discovery & Snapshot Sistem

## 1. Kontekst i cilj projekta

ZORA je do sada bio fokusiran na uzak set unapred odabranih CEX-DEX parova (SOL, RAY, BONK, ZEC, UNI), sa direktnim praćenjem spread-a u realnom vremenu preko SHM arhitekture. Paper trading je pokazao da većina tih parova nikad ne dostiže net profitabilan spread threshold — problem nije bio u egzekuciji, već u **izboru parova**. Birali smo parove intuicijom (poznati tokeni, visok market cap), a ne podacima.

Cilj ovog projekta je obrnuti taj redosled: umesto da pretpostavljamo koji parovi imaju edge i onda to proveravamo, prvo gradimo **bazu koja pokriva širok presek tržišta** — mnogo poolova, mnogo tokena, mnogo CEX menjačnica — i onda tu bazu koristimo da statistički otkrijemo gde se spread stvarno dešava, pod kojim uslovima, i koliko traje.

Radna hipoteza je da najveći edge ne živi u velikim, dobro pokrivenim parovima (SOL/USDC, BTC/USDT) gde je konkurencija arbitraža ogromna i spread se zatvara u milisekundama, već u **long-tail** delu tržišta: sveže kreirani poolovi, tanka likvidnost, novi CEX listinzi gde tržišni maker-i i arbitri još nisu stigli da uspostave efikasnost. Ovo je direktno analogno nalazu iz GEX projekta — edge nije u prosečnom efektu preko celog uzorka, već u specifičnim segmentima i uslovima (tamo: blizina zero-gamma nivoa; ovde: verovatno starost pool-a, TVL bucket, venue, tip listinga).

Faza 1, opisana u ovom dokumentu, ne traga još za tim edge-om. Faza 1 gradi **infrastrukturu za posmatranje** — čist, širok, vremenski obeležen snapshot DEX i CEX univerzuma, dovoljno dug i dovoljno gust da statistika u kasnijim fazama ima na čemu da radi.

## 2. Metodologija

Pristup je namerno konzervativan po pitanju filtriranja na ulazu i agresivan po pitanju širine pokrivenosti:

- **Širina pre selekcije.** Ne biramo tokene/poolove unapred na osnovu subjektivne procene "ovo izgleda zanimljivo". Snimamo sve što prođe minimalan sanity filter (izbacujemo samo potpune duhove — poolove sa nula volumena, mrtve simbole). Selekcija i pametno filtriranje dešavaju se u analitičkom sloju (Faza 3), ne u ingestion sloju. Razlog: long-tail signal je po definiciji u repu distribucije koji bi agresivan filter na ulazu odsekao pre nego što ga i vidimo.

- **Adaptivna gustina posmatranja, ne uniformna.** Ne posmatramo ceo univerzum istom frekvencijom. Većina poolova je stabilna i "dosadna" — puna 15-min ili 5-min rezolucija je za njih dovoljna. Mali podskup — sveže kreirani poolovi, novi CEX listinzi, nagli skokovi volumena/TVL-a — dobija gušću, ciljanu pažnju, jer je to segment gde po radnoj hipotezi long-tail edge najverovatnije živi. Ovo direktno odražava GEX princip: ne trošiti isti budžet pažnje/resursa na ceo uzorak kad znaš da signal verovatno živi u specifičnom segmentu.

- **Novi entiteti kao prvoklasni signal.** Trenutak pojavljivanja novog pool-a ili novog CEX listinga tretiramo kao potencijalno najvredniji podatak u celom sistemu, ne kao sporedan metapodatak. Eksplicitna `new_entity_events` tabela postoji upravo zato da kasnija analiza može direktno da pita: "šta se dešava sa spread-om u prvih N sati/dana od pojavljivanja tokena, i da li ta dinamika opada, i kojom brzinom."

- **Odvajanje slojeva.** Faza 1 (ovaj dokument) ne dira CEX-DEX matching, ne računa spread, ne radi statistiku. Svaki sloj gradimo i validiramo nezavisno, isto kao što je GEX pipeline građen inkrementalno (parse → fetch → merge → signali → backtest). Ovo nam daje jasne tačke provere pre nego što uložimo vreme u sledeći, skuplji sloj.

- **Nezavisnost od konkretnog izvora podataka.** Izvor podataka (REST API, RPC, Geyser stream) je implementacioni detalj, ne arhitektonska odluka. Sistem se projektuje tako da promena izvora u kasnijoj fazi (npr. REST → RPC → Geyser zbog manje latencije) ne zahteva izmenu matching, spread computation ili analitičkog sloja.

## 3. Šta Faza 1 jeste — obim rada

Periodičan, adaptivno-gust snapshot celog relevantnog DEX i CEX univerzuma (poolovi, tokeni, osnovne metrike likvidnosti/volumena), upisan u ClickHouse, sa automatskom detekcijom novopojavljenih entiteta.

### 3.1 Izvori podataka

**DEX venue-i:**
- Orca Whirlpool
- Raydium AMM v4
- Raydium CLMM
- Meteora DLMM

Podaci se povlače preko public REST API-ja svakog protokola (Orca API, Raydium `api-v3`, Meteora API) za baseline (Tier 1) posmatranje. Direktan RPC (`getMultipleAccounts`) koristi se ciljano za Tier 2 (hot) poolove — videti sekciju 3.2 i 3.3. Direktan RPC `getProgramAccounts` scan preko celog univerzuma koristi se samo povremeno kao fallback/cross-check pokrivenosti REST izvora.

**CEX menjačnice:**
- Binance
- Bybit
- OKX
- Bitget
- Gate.io

Kriterijum izbora: kombinacija duboke likvidnosti (Binance, Bybit, OKX) i brzine/širine novih listinga (Bitget, Gate.io). Binance, Bybit i OKX daju nam realan CEX leg za kasniju procenu izvodljivosti arbitraže; Bitget i Gate.io proširuju token universe i hvataju nove listinge koje veće menjačnice često promaše ili listuju kasnije.

Podaci se povlače preko standardnih `exchangeInfo`/`instruments` REST endpoint-a, uniformnim adapterom po menjačnici koji sve mapira na isti izlazni format.

### 3.2 Scheduling — Tiered Adaptive Polling

Umesto jedinstvenog fiksnog intervala za sve poolove, sistem koristi tri nivoa pažnje. Tier 3 je definisan konceptualno u ovoj fazi, ali se ne implementira dok Faza 2/3 ne obezbede stvarne spread podatke na kojima bi se zasnivao.

**Tier 1 — Discovery (5 min)**
Ceo DEX i CEX univerzum, bez izuzetka. Cilj: postojanje, TVL, volume, fee, price, status. Ovo je baseline sloj — dovoljno gust da uhvati evoluciju stabilnih poolova, dovoljno redak da ne generiše šum (agregirani metrici kod provajdera se često keširaju 5-60s pa i češće osvežavanje ne bi donelo dodatnu informaciju — videti sekciju 5.3).

**Tier 2 — Hot / New Entity (1 min)**
Poolovi promovisani u ovaj tier po dva nezavisna mehanizma, koja se namerno ne mešaju u jedinstvenu logiku:

- *Age-based (fiksni prozor, bez re-evaluacije).* Pool ulazi u Tier 2 čim se prvi put detektuje kao nov (preko `new_entity_events`) i ostaje tu fiksnih **72h**, bez obzira na to šta mu se dešava sa aktivnošću u međuvremenu. Namerno se ne demotuje raniji ako mu volumen padne na nulu — kriva opadanja aktivnosti novog pool-a (ili njeno odsustvo) je sama po sebi deo signala koji Faza 3 treba da vidi, ne šum koji treba filtrirati pre analize.
- *Growth-based (kontinuirano re-scoring, sa histerezisom).* Nezavisno od starosti, bilo koji pool može ući u Tier 2 zbog naglog skoka volumena ili TVL-a, ili zbog novog CEX listinga za taj token. Ovo se re-evaluira na svaki Tier 1 ciklus (5 min). Da bi se izbeglo flapping (ulazak/izlazak svaki ciklus zbog šuma u merenju), democija zahteva **N uzastopnih ciklusa** (predlog: 3× = 15 min) ispod praga pre nego što pool napusti Tier 2.

Svaki upis u Tier 2 nosi **razlog ulaska** (`hot_reason`), ne samo binarnu zastavicu — jer je razlog sam po sebi koristan feature za kasniju segmentiranu statistiku (isti princip kao segment-level analiza u GEX projektu, gde prosečan efekat preko celog uzorka sakriva efekat vidljiv samo u specifičnom segmentu).

Za growth-based promociju koristi se **priority_score** — heuristika, ne finalni signal:

```
priority_score = w1 * is_new_cex_listing
               + w2 * normalized(volume_growth_24h)
               + w3 * normalized(tvl_growth_24h)
               + w4 * venue_count
```

Bitne napomene o ovoj formuli:
- Heterogene komponente (binarne zastavice, procenti neograničenog opsega, mali celi brojevi) moraju biti normalizovane (z-score ili percentile rank unutar snapshot ciklusa) pre sabiranja — bez toga volume_growth dominira score-om jer mu je opseg vrednosti ogroman u odnosu na ostale članove, i formula postaje de facto čist proxy za volume spike.
- Znak i težina člana `venue_count` nisu unapred očigledni. Token na više venue-a je verovatno zreliji i efikasniji (manji edge), ali to je pretpostavka koju treba testirati na stvarnim spread podacima u Fazi 3, ne unapred pretpostaviti.
- Težine (`w1..w4`) se **ne kalibrišu u Fazi 1**. Ovaj score služi isključivo za alokaciju resursa (koji pool zaslužuje Tier 2 pažnju), ne kao trgovački signal. Za početak je dovoljno jednostavno OR pravilo (npr. `is_new_listing OR volume_growth > 3x OR tvl_growth > 2x`) umesto finog ponderisanja. Prava kalibracija (moguće i kroz naučen model, npr. logističku regresiju, umesto ručnih težina) dolazi tek kad Faza 3 ima dovoljno istorije da poveže "šta je bilo u hot tier-u" sa "gde se spread stvarno desio".

**Tier 3 — Candidate (definisan konceptualno, implementacija odložena)**
Poolovi za koje Faza 2/3 statistički pokažu da: spread prelazi zadati prag, prelazi ga sa merljivom učestalošću, ili statistički odskaču od proseka svog segmenta. Ovaj tier dobija sopstvenu (verovatno još gušću ili event-driven) strategiju posmatranja, definisanu tek kad postoji podatak koji je opravdava.

### 3.3 Apstraktni sloj pristupa podacima (Provider abstraction)

Da bi promena izvora podataka u kasnijim fazama (REST → RPC → Geyser/Yellowstone) bila jeftina i ne zahtevala izmenu matching, spread computation ili analitičkog sloja, pristup podacima o poolovima se ne vezuje direktno za konkretnu REST implementaciju. Umesto jednog monolitnog interfejsa, apstrakcija se deli **po tipu podatka koji izvor može da isporuči**, ne po venue-u — jer nijedan pojedinačan izvor (REST, RPC ili Geyser) ne pokriva sve podatke podjednako dobro:

```
PoolMetadataProvider   -- sporo-menjajuće: mint-ovi, fee tier, da li pool postoji
PoolStateProvider      -- brzo-menjajuće: price, active_liquidity (iz sirovog on-chain stanja)
PoolVolumeProvider     -- agregirano: volume_24h_usd, TVL (računa ih indexer iz istorije swap-ova)
```

Praktična posledica ove podele za Tier 2: RPC (`getMultipleAccounts`, batch do 100 adresa po pozivu) implementira samo `PoolStateProvider` — može da vrati trenutni `price`/`active_liquidity` direktno iz account bajtova, ali **ne** i `volume_24h_usd`, jer je to off-chain agregacija koju sirov account state ne sadrži. REST implementacija (Orca/Raydium/Meteora API) implementira sva tri interfejsa odjednom, jer provajderov endpoint već vraća agregirane vrednosti u istom pozivu.

Zbog toga Tier 2 posmatranje kombinuje dva izvora, ne prelazi potpuno na RPC:
- RPC `PoolStateProvider` na 1-min kadenci za `price` i `active_liquidity`
- REST `PoolVolumeProvider` na baseline (5-min) kadenci samo za `volume_24h_usd`, jer je to po prirodi spor metrik kome gušće osvežavanje ne bi donelo dodatnu informaciju

Ova podela takođe direktno koristi postojećoj ZORA infrastrukturi — Rust DEX ingestor već radi batched `getMultipleAccounts` čitanje za Raydium AMM v4; Tier 2 RPC provider je prirodna ekstenzija istog pristupa, proširena na CLMM/DLMM `sqrt_price_x64` parsing (Orca Whirlpool, Raydium CLMM, Meteora DLMM), ne novi paradigm.

Isti princip primenjuje se simetrično na CEX stranu (`SymbolProvider`, budući `CexPriceProvider` za kasniju spread fazu) — REST `exchangeInfo` poziv se ne hardkoduje direktno u scheduler, već ide kroz isti tip apstrakcije.

**Napomena o push vs pull izvorima.** REST i RPC su oba pull modeli (klijent traži, server odgovara na fiksnom intervalu). Geyser/Yellowstone je push (server šalje update kad se account promeni, klijent ne kontroliše frekvenciju) — ovo nije samo drugačija implementacija istog trait-a, već drugačiji execution model. Za Fazu 1 ovo se ne rešava (samo REST se implementira, pull je dovoljan), ali se ostavlja kao arhitektonska napomena: budući Geyser provider će verovatno sedeti iza tanke adapter komponente koja pretvara push stream u isti normalizovan `PoolState` oblik koji ostatak sistema očekuje, umesto da se trait dizajnira striktno kao `async fn fetch(...)` i onda "silom" prilagođava push modelu.

Namerno se ne projektuje dalje od ove tri podele (metadata/state/volume) i push/pull napomene — dodatna generalizacija bez konkretne potrebe je preuranjena optimizacija (YAGNI). REST implementacije triju trait-ova su jedino što se gradi u Fazi 1; RPC (Tier 2 state) i Geyser implementacije dolaze kad im dođe red.

### 3.4 Baza podataka

ClickHouse. Izbor je namerno drugačiji od QuestDB (koji se razmatra za ZORA/GEX real-time tick podatke) jer je ovde workload analitički — agregacije po TVL bucket-u, distribucije spread-a po uslovima, window funkcije nad širokim skupom entiteta — a ne visoko-frekventan single-series upis. Ako sistem kasnije preraste u execution fazu sa tick-level real-time zahtevima, taj sloj dobija svoju QuestDB instancu; ClickHouse ostaje analitički sloj.

### 3.5 Šema

```sql
CREATE TABLE dex_pool_snapshots (
    snapshot_ts        DateTime64(3),
    venue               LowCardinality(String),  -- orca_whirlpool, raydium_amm_v4, raydium_clmm, meteora_dlmm
    pool_address        String,
    base_mint            String,
    base_symbol          LowCardinality(String),
    quote_mint           String,
    quote_symbol         LowCardinality(String),
    fee_bps             UInt16,
    tvl_usd             Float64,
    active_liquidity    Float64,   -- likvidnost u aktivnom tick/bin range-u (CLMM/DLMM); = tvl_usd za AMM v4
    volume_24h_usd       Float64,
    price_native         Float64,   -- cena 1 base u quote jedinicama, direktno iz pool state-a, bez eksterne konverzije — autoritativna za Fazu 2 matching
    price_usd            Nullable(Float64),  -- provider-ova USD konverzija (ako je API nudi); samo za display/Tier1 filtriranje, NIJE osnova za spread computation
    tier                LowCardinality(String),  -- 'discovery' | 'hot'
    hot_reason           LowCardinality(String)   -- '' | 'new_pool' | 'volume_spike' | 'tvl_spike' | 'new_listing'
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(snapshot_ts)
ORDER BY (venue, pool_address, snapshot_ts)
TTL toDateTime(snapshot_ts) + INTERVAL 90 DAY;

CREATE TABLE cex_symbol_snapshots (
    snapshot_ts     DateTime64(3),
    exchange        LowCardinality(String),  -- binance, bybit, okx, bitget, gateio
    symbol          String,
    base_asset      LowCardinality(String),
    quote_asset     LowCardinality(String),
    status          LowCardinality(String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(snapshot_ts)
ORDER BY (exchange, symbol, snapshot_ts)
TTL toDateTime(snapshot_ts) + INTERVAL 90 DAY;

CREATE TABLE new_entity_events (
    detected_ts     DateTime64(3),
    entity_type     LowCardinality(String),  -- dex_pool | cex_symbol
    venue            LowCardinality(String),
    identifier       String,                  -- pool_address ili symbol
    metadata         String                   -- JSON blob
) ENGINE = MergeTree()
ORDER BY (detected_ts, entity_type);
-- bez TTL, čuva se trajno

CREATE TABLE known_entities (
    entity_type     LowCardinality(String),  -- dex_pool | cex_symbol
    venue            LowCardinality(String),
    identifier       String,
    first_seen_ts    DateTime64(3)
) ENGINE = MergeTree()
ORDER BY (entity_type, venue, identifier);
-- append-only registry, insert samo jednom po identifier-u (nikad update),
-- bez TTL, bez last_seen polja — jedina odgovornost je "da li smo ovo ikad videli"
```

**Napomena o `active_liquidity`:** za CLMM/DLMM poolove (Orca Whirlpool, Raydium CLMM, Meteora DLMM), ukupan TVL ne govori dovoljno o dubini tržišta oko trenutne cene — likvidnost može biti koncentrisana daleko od spot cene (neaktivan LP). `active_liquidity` beleži likvidnost u trenutno aktivnom tick/bin range-u, izvučenu iz `liquidity` polja u pool account state-u. Za AMM v4 poolove (constant product), TVL i efektivna likvidnost su ekvivalentni, pa se polje popunjava istom vrednošću.

**Napomena o `base_mint`/`quote_mint` i `price_native`/`price_usd`:** redosled kojim provider API (ili sirov account state) vraća dva tokena u pool-u nije pouzdan indikator ko je "quote" — isti logički par (npr. TOKEN/SOL) može doći u obrnutom redosledu na različitim pool-ovima, čak i unutar istog venue-a. Šema zato eksplicitno kanonizuje stranu: ako je jedna strana poznati stablecoin (USDC/USDT), ona je quote; inače, ako je jedna strana SOL, SOL je quote; ostali, egzotičniji slučajevi rešavaju se pojedinačno u Fazi 2 kad se gradi puna token taksonomija. `price_native` je sirova cena baze u quote jedinicama, direktno iz pool state-a, bez ikakve eksterne konverzije — ovo je autoritativna vrednost za Fazu 2 matching. `price_usd`, ako ga provider nudi, je posebno polje isključivo za display i Tier 1 filtriranje (npr. brz pregled TVL-a u dolarima); namerno se ne koristi kao osnova za spread computation, jer bi već prošao kroz providerov sopstveni, nezavisan SOL/USD (ili drugi) mark, snimljen u nepoznatom trenutku — mešanje te konverzije sa CEX cenom iz posve drugog izvora u Fazi 2 bi u spread signal ubacilo šum ili sistematsku pristrasnost koja nema veze sa stvarnom tržišnom neefikasnošću.

**Napomena o `TTL toDateTime(snapshot_ts) + INTERVAL 90 DAY`:** potvrđeno tokom Faze 1a implementacije da ClickHouse 24.8 odbija TTL izraz direktno nad `DateTime64(3)` kolonom (`BAD_TTL_EXPRESSION`). Kolona `snapshot_ts` ostaje `DateTime64(3)` — milisekundska preciznost se ne gubi — samo TTL bucketing izraz zahteva eksplicitan `toDateTime()` cast. Ovo je jedino mesto gde implementacija odstupa od doslovnog SQL-a u ovom dokumentu, i odstupanje je minimalno i namerno.

**Napomena o null/netagovanim token simbolima:** ~0.8% Orca pool-ova vraća `None` za simbol umesto da pool izostane iz odgovora (netagovan/nelistiran token, ne nepostojeći pool). U skladu sa principom iz sekcije 3.7 ("širina pre selekcije"), ovi pool-ovi se ne izbacuju — `None` se mapira na prazan string (`""`), konzistentno sa postojećom konvencijom praznog kategorijalnog polja (`hot_reason`'s `''`). Ovaj obrazac treba primeniti uniformno u svakom budućem REST provideru koji naiđe na isti slučaj, ne samo u Orca adapteru. da bi se `price_native` (npr. TOKEN/SOL) ispravno normalizovao na USD i uporedio sa CEX cenom (tipično TOKEN/USDT), Fazi 2 je potreban pouzdan, vremenski uparen istorijski tick za SOL/USD(T) iz istog snapshot intervala. Trenutna šema to ne obezbeđuje: `cex_symbol_snapshots` namerno ne nosi cenu (samo `status`), a postojeći ZORA CEX ingestor piše cene samo u efemeran SHM, ne u perzistentnu bazu. Dve moguće opcije za Fazu 2 — (a) dodati laganu, Tier-1-kadence snimku cene za mali skup ključnih quote asset-a (SOL, USDC, USDT) u ovaj isti pipeline, ili (b) osloniti se na poseban perzistentan CEX price log (npr. proširenje ranije pomenutog standalone Python spread monitora da piše i u ClickHouse) — ostaje otvoreno i odlučuje se pri dizajnu Faze 2, ne ovde.

**Napomena o `tier`/`hot_reason`:** ova dva polja beleže pod kojim režimom posmatranja je konkretan red nastao (Tier 1 baseline vs Tier 2 sa specifičnim razlogom ulaska). Ovo nije samo interni bookkeeping detalj scheduler-a — to je feature koji Faza 3 statistika direktno koristi za segmentaciju (npr. "da li je spread verovatnoća drugačija za poolove promovisane zbog volume_spike naspram new_pool").

Bookkeeping trenutnog stanja Tier 2 liste (koji pool je trenutno hot, od kada, po kom razlogu, koliko uzastopnih ciklusa je ispod praga za demociju) vodi se odvojeno od ovih append-only snapshot tabela — kao interno stanje scheduler procesa, ne kao deo istorijske šeme.

### 3.6 Detekcija novih entiteta

Detekcija se namerno ne radi anti-joinom protiv *prethodnog* snapshot-a, niti upitom nad celom `dex_pool_snapshots`/`cex_symbol_snapshots` istorijom. Prvi pristup daje lažne pozitivne rezultate čim REST API jednom promaši pool zbog timeout-a/rate limita pa ga vrati u sledećem ciklusu (isti pool bi se ponovo prijavio kao "nov"). Drugi pristup ima dva problema: fact tabele imaju 90-dnevni TTL, pa se pool koji je bio tih duže od 90 dana pogrešno prijavljuje kao nov iako je već viđen; i upit nad rastućom fact tabelom (desetine miliona redova) vremenom sve više usporava, tiho degradirajući scheduler.

Umesto toga, koristi se posebna, kompaktna **registry tabela** (`known_entities`) čija je jedina odgovornost odgovor na pitanje "da li smo ovo ikad videli":

1. Na svaki Tier 1 ciklus, batch upit proveri koji od trenutno viđenih identifikatora (po venue-u) već postoje u `known_entities`.
2. Za one koji ne postoje: insert u `known_entities` (jednom, zauvek) + upis u `new_entity_events`, što okida age-based promociju u Tier 2.
3. Za one koji već postoje: nema upisa — registry se ne dira.

Ovaj insert-once pattern drži `known_entities` malom (raste samo sa brojem *novih* entiteta ikad viđenih, ne sa brojem svih snapshot redova) i tabela ostaje brza bez obzira koliko fact tabele narastu. Namerno se ne koristi `ReplacingMergeTree`: pošto se isti identifier nikad ne upisuje drugi put, nema duplikata koje treba dedup-ovati na merge-u, pa taj mehanizam rešava problem koji ovaj dizajn i nema — obična `MergeTree` je dovoljna i izbegava nedeterminizam ne-`FINAL` upita nad tabelom čiji merge nije odmah izvršen.

`known_entities` namerno ne nosi `last_seen` polje. Praćenje "kad je entitet poslednji put viđen" je odvojena odgovornost od "da li je entitet nov", sa sopstvenom cenom održavanja (periodično osvežavanje ili mutacije) koja nema veze sa detekcijom novih entiteta. Ako to zatreba kasnije (npr. u Fazi 3, za analizu perioda tišine pre reaktivacije pool-a), računa se na zahtev iz fact tabela (`max(snapshot_ts)` grupisano po identifikatoru, u okviru 90-dnevnog prozora) ili kroz posebnu, odvojeno održavanu tabelu — ne dodaje se u registry.

**Poznato ograničenje:** check-then-insert pattern pretpostavlja jedan sekvencijalni scheduler proces. Ako ingestion ikad postane paralelan po venue-u, dva konkurentna procesa teoretski mogu istovremeno proveriti da entitet ne postoji i oba ga upisati, dupliranjem reda u `known_entities` i `new_entity_events`. Nebitno za Fazu 1 (jedan scheduler proces), ali vredi zapamtiti kao pretpostavku ako se arhitektura kasnije paralelizuje.

### 3.7 Filtriranje na ulazu

Minimalno i namerno permisivno: odsecamo samo očigledne duhove (npr. poolovi sa nula 24h volumena u trenutku snimanja). Ne postavljamo TVL prag koji bi izbacio male/nove poolove — to je upravo segment koji nas najviše zanima za long-tail hipotezu. Suptilnije filtriranje (TVL bucket-i, minimalna starost, minimalan volume za "ozbiljne" kandidate) radi se u query/analitičkom sloju, ne pri upisu.

### 3.8 Retencija

90 dana pune granularnosti za oba snapshot tabele (Tier 1 na 5 min, Tier 2 na 1 min dok je pool aktivno hot), preko ClickHouse TTL-a. `new_entity_events` čuva se trajno bez TTL-a (mala, retko upisivana tabela, vredna kao trajna istorija "kad se šta pojavilo"). Ako se kasnije pokaže potreba za dužom istorijom, dodaje se materialized view koji agregira stare redove na grublju granularnost pre isteka TTL-a — nije nepovratna odluka.

## 4. Šta Faza 1 namerno NE radi

Ovo je eksplicitno van obima da bi se izbeglo mešanje slojeva:

- **Token mint ↔ CEX ticker matching.** Povezivanje istog tokena preko DEX mint adrese i CEX simbola je zaseban, netrivijalan problem (kolizije simbola, wrapped verzije, itd.) i radi se u Fazi 2.
- **Spread computation.** Računanje CEX-DEX cenovne razlike zahteva već urađen matching iz Faze 2 — logički ne može ranije.
- **Statistička analiza.** Distribucije, uslovljene verovatnoće, permutation testovi na pitanje "pod kojim uslovima se spread dešava" dolaze u Fazi 3, nakon što imamo dovoljno istorije spread podataka iz Faze 2.
- **Kalibracija priority_score težina.** Score se koristi samo kao gruba heuristika za Tier 2 alokaciju resursa u Fazi 1; fino podešavanje težina (ili zamena naučenim modelom) čeka stvarne spread podatke iz Faze 2/3.
- **Tier 3 implementacija.** Definisan konceptualno, implementira se tek kad Faza 2/3 daju statističku osnovu za to koji poolovi zaslužuju taj nivo pažnje.
- **Egzekucija.** Bilo kakvo aktivno trgovanje ili čak paper trading na osnovu ovih podataka dolazi tek nakon što Faza 3 pokaže da postoji statistički konzistentan (ne samo naivan, autokorelacijom iskrivljen) signal — ista disciplina koja je primenjena u GEX projektu.

## 5. Rate limiti i implikacije na scheduler dizajn

_Pregled ažuriran avgust 2026; limiti se menjaju bez najave i treba ih periodično proveravati protiv zvanične dokumentacije pre produkcijske upotrebe._

### 5.1 DEX pool API-jevi

| DEX | Rate limit | Osnova | Napomena |
|---|---|---|---|
| Orca | Nedefinisan broj, IP-implicitan | IP | Bez auth-a. Osloniti se na exponential backoff pri 429. |
| Raydium | ~120 req/min orijentaciono za transakcione endpointe (nezvaničan savet) | Progresivno po IP (Cloudflare) | Odvojenih 11 servisa na različitim hostovima; read pozivi keširani 5–60s na edge-u. Koristiti batch endpoint (`/pools/info/ids?ids=...`) — Raydium ima najveći broj poolova od sva četiri DEX-a. |
| Meteora | 30 req/s, fiksno | Globalno po DLMM API-ju | Jedini DEX sa jasno objavljenim brojem — najlakše za planiranje kapaciteta. |

### 5.2 CEX API-jevi (public/market data endpointi relevantni za Fazu 1)

| Berza | Public limit | Osnova | Kazna za prekoračenje |
|---|---|---|---|
| Binance | 6.000 weight/min | Po IP | 429 → 418 IP ban, 2 min do 3 dana, eskalira |
| Bybit | 600 req/5s (default) | Po IP | 403 "access too frequent" → ~10 min čekanje |
| OKX | ~20 req/2s po tipu endpointa; globalni plafon 250 req/s | Po IP | 429, error kod 50061 |
| Bitget | 20 req/s (unified market data) | Po IP | 429 |
| Gate.io | 200 req/10s po endpointu | Po IP | 429 |

### 5.3 Šta ovo znači za Fazu 1

- **Ni Tier 1 (5 min) ni Tier 2 (1 min) frekvencija nisu blizu bilo kog navedenog limita** — svi izvori (REST i RPC) imaju dovoljno kapaciteta da pokriju ceo univerzum na ovim intervalima sa velikom marginom. RPC `getMultipleAccounts` dodatno batch-uje do 100 adresa po pozivu, pa čak i stotine Tier 2 poolova zahtevaju samo par poziva u minuti, potpuno nezavisno od REST rate limita.
- **Najveći praktični rizik nije normalna operacija, već retry storm** — ako baza ili mreža privremeno otkažu i scheduler pokuša da nadoknati propušten ciklus agresivnim ponavljanjem poziva, Binance/OKX IP ban (do 3 dana) bi obustavio ceo CEX snapshot sloj za sve menjačnice sa istog izlaznog IP-ja, ne samo za tu jednu berzu. Zato je token-bucket limiter po servisu obavezan deo implementacije, ne naknadna optimizacija.
- **WebSocket se ne broji u REST rate limit ni na jednoj CEX platformi**, ali za Fazu 1 (5-min snapshot simbola, ne tick podataka) WS nije neophodan — `exchangeInfo` poziv na ovoj frekvenciji je zanemarljiv teret. WS ostaje relevantan za kasnije faze (real-time spread praćenje), ne za discovery sloj.
- **Provajderi već keširaju agregirane metrike na svojoj strani** (npr. Raydium 5-60s na edge-u) — ovo je dodatni argument, pored samog rate limita, da Tier 1 na 5 min (a ne agresivnije) ne gubi informaciju za stabilne poolove: gušće pollovanje bi često vraćalo identičnu, kešfiguranu vrednost.
- **Gate.io i Bybit dodatno kažnjavaju loš fill ratio** na order endpointima — nebitno za Fazu 1 (samo čitamo market data, ne šaljemo ordere), ali vredi zabeležiti za kasniju execution fazu.

### 5.4 Implementacione smernice za scheduler

1. Svaki servis (5 CEX + 4 DEX = 9 izvora, plus RPC za Tier 2) dobija sopstveni token-bucket rate limiter, podešen na ~90% objavljenog/orijentacionog limita kao sigurnosnu marginu.
2. Poštovati `Retry-After` header gde postoji (Binance, Bybit); za servise bez njega (Orca) koristiti exponential backoff sa gornjom granicom.
3. Čitati i logovati rate-limit header-e iz odgovora radi vidljivosti pre nego što se dođe do 429: Binance (`X-MBX-USED-WEIGHT-*`), Bybit (`X-Bapi-Limit-Status`, `X-Bapi-Limit`), OKX (`x-ratelimit-limit`, `x-ratelimit-remaining`).
4. Koristiti batch/multi-ID endpoint gde god postoji (Raydium `/pools/info/ids`, RPC `getMultipleAccounts`) umesto pojedinačnih poziva po pool-u.
5. Scheduler treba da tretira svaki servis nezavisno — pad ili throttling jednog izvora (npr. Meteora API nedostupan) ne sme blokirati upis snapshot-a za preostale izvore.
6. Tier 2 promocija/democija (age-based i growth-based) evaluira se kao deo Tier 1 ciklusa (svakih 5 min), ne kao poseban tajmer — pojednostavljuje orkestraciju i garantuje da je scoring uvek zasnovan na sveže upisanom Tier 1 snapshot-u.

## 6. Kriterijum uspeha za Fazu 1

Faza 1 se smatra završenom kada:

1. Svih 5 CEX menjačnica i 4 DEX venue-a upisuju konzistentne, validne Tier 1 snapshot-e na 5-min intervalu bez ručne intervencije kroz najmanje 48h neprekidnog rada.
2. Tier 2 mehanizam radi ispravno: age-based promocija drži pool tačno 72h bez preranog gašenja, growth-based promocija/democija reaguje na stvarne skokove/padove sa histerezisom (bez vidljivog flapping-a u logovima).
3. `new_entity_events` tabela pouzdano hvata novopojavljene poolove i simbole (verifikovano ručnim cross-checkom protiv poznatih novih listinga u tom periodu).
4. `tier` i `hot_reason` polja su ispravno popunjena i konzistentna sa internim stanjem scheduler-a u trenutku upisa.
5. Osnovni upiti nad bazom (broj aktivnih poolova po venue-u, distribucija TVL-a, lista novih entiteta u zadnjih 24h, trenutna Tier 2 lista sa razlozima) rade brzo i vraćaju očekivane rezultate.

Tek nakon toga ima smisla preći na Fazu 2 (matching) — bez pouzdanog i širokog snapshot sloja, svaki naredni sloj gradi se na nesigurnim temeljima.

---

Ovim je Faza 1 konceptualno zatvorena: obim, šema, adaptivno pollovanje, apstrakcija izvora podataka, detekcija novih entiteta i normalizacija cena su definisani u meri dovoljnoj za implementaciju. Otvorena pitanja koja namerno prelaze u Fazu 2 (token mint ↔ CEX ticker matching, perzistentan CEX price log za normalizaciju, kalibracija priority_score, Tier 3) su eksplicitno zabeležena, ne zaboravljena — svako od njih čeka podatke ili odluke koje Faza 1 sama po sebi ne može da obezbedi.
