# OTel Standards Alignment: Script vs Separate Package

## Question: Does using a script align with OpenTelemetry standards?

**Short Answer:** ❌ No, the script approach does NOT align with OTel standards. The separate package approach is the **only standards-compliant method**.

---

## Script Approach (Current) - ❌ NOT Standards Compliant

### What It Does

```bash
# Makefile
genotelcontribcol: $(BUILDER)
    $(BUILDER) --skip-compilation --config builder-config.yaml
    ./internal/buildscripts/add-query-cache-extension.sh  # ← Post-processing hack
```

The script **modifies generated code** after the builder completes:

```bash
# Script modifies components.go
perl -i -pe 's/(healthcheckextension\.NewFactory\(\),)/
    $1\n\t\tnewrelicsqlserverreceiver.NewExtensionFactory()/' components.go
```

---

### Why It Violates Standards

#### 1. **Generated Code Should Not Be Modified**

**OTel Principle:** The builder generates final, correct code. Modifications indicate a design problem.

```
❌ Builder generates code
❌ Script modifies generated code
❌ Final code differs from builder's intent
```

**From OTel Collector Builder docs:**
> "The generated code should not be manually edited. All changes should come through the builder configuration."

---

#### 2. **Package Structure Violation**

**OTel Standard:** Each component type has dedicated directories.

```
✅ receiver/     → Receivers only
✅ processor/    → Processors only
✅ exporter/     → Exporters only
✅ extension/    → Extensions only
❌ receiver/newrelicsqlserverreceiver/cache_extension.go  → WRONG!
```

**Current structure violates separation:**
```
receiver/newrelicsqlserverreceiver/
├── scraper.go              # ✅ Receiver logic
├── factory.go              # ✅ Receiver factory
├── cache_extension.go      # ❌ Extension in receiver package
└── NewExtensionFactory()   # ❌ Extension factory in receiver package
```

**All 60+ official extensions follow this:**
```
extension/
├── healthcheckextension/   # ✅ Separate package
├── basicauthextension/     # ✅ Separate package
├── datadogextension/       # ✅ Separate package
└── [60+ more extensions]   # ✅ All separate packages
```

---

#### 3. **Builder Configuration Incomplete**

**OTel Standard:** All components must be declared in builder config.

**Current builder-config.yaml:**
```yaml
extensions:
  - gomod: .../extension/healthcheckextension v0.141.0
  # ❌ querycache extension NOT listed
```

**Builder is unaware of querycache:**
- ❌ No dependency tracking
- ❌ No version management
- ❌ No validation
- ❌ Not in build manifest

---

#### 4. **Factory Naming Conflict**

**OTel Convention:** One `NewFactory()` per package.

**Current:**
```go
// receiver/newrelicsqlserverreceiver/factory.go
func NewFactory() receiver.Factory { ... }  // Receiver factory

// receiver/newrelicsqlserverreceiver/cache_extension.go
func NewExtensionFactory() extension.Factory { ... }  // ❌ Non-standard name
```

**Why NewExtensionFactory() is non-standard:**
- ❌ Not the conventional `NewFactory()`
- ❌ Builder expects `NewFactory()` in each package
- ❌ Indicates architectural problem

**Standard pattern:**
```go
// extension/querycache/factory.go
func NewFactory() extension.Factory { ... }  // ✅ Standard name
```

---

#### 5. **No Reusability**

**OTel Principle:** Extensions should be reusable across components.

**Current:**
```go
import "github.com/.../receiver/newrelicsqlserverreceiver"  // ❌ Must import receiver

// Oracle receiver wants to use cache?
// ❌ Must import SQL Server receiver package (wrong!)
```

**Standard:**
```go
import "github.com/.../extension/querycache"  // ✅ Import extension directly

// Any receiver can use it
// ✅ Clean dependency
```

---

## Separate Package Approach - ✅ FULLY Standards Compliant

### Proper Structure

```
extension/querycache/           # ✅ Standard location
├── extension.go                # Extension implementation
├── factory.go                  # func NewFactory()
├── config.go                   # Configuration types
├── cache.go                    # Cache logic
├── go.mod                      # Module definition
├── go.sum                      # Dependency lock
├── README.md                   # Documentation
├── metadata.yaml               # Component metadata
└── testdata/
    └── config.yaml             # Example config
```

---

### Builder Configuration

**builder-config.yaml:**
```yaml
extensions:
  - gomod: github.com/open-telemetry/opentelemetry-collector-contrib/extension/healthcheckextension v0.141.0
  - gomod: github.com/open-telemetry/opentelemetry-collector-contrib/extension/querycache v0.141.0  # ✅ Explicit
```

**Builder automatically:**
- ✅ Discovers `NewFactory()` function
- ✅ Generates correct registration code
- ✅ Manages dependencies
- ✅ Creates build manifest

---

### Generated Code (Automatic)

**components.go** (generated by builder):
```go
import (
    healthcheckextension "github.com/.../extension/healthcheckextension"
    querycache "github.com/.../extension/querycache"  // ✅ Auto-imported
)

factories.Extensions, err = otelcol.MakeFactoryMap[extension.Factory](
    healthcheckextension.NewFactory(),
    querycache.NewFactory(),  // ✅ Auto-registered
)
```

**No script needed!**

---

## Standards Comparison Table

| Requirement | Script Approach | Separate Package |
|-------------|-----------------|------------------|
| **Package Structure** | ❌ Mixed receiver+extension | ✅ Separated by type |
| **Builder Integration** | ❌ Post-processing hack | ✅ Native support |
| **Generated Code** | ❌ Modified after generation | ✅ Generated correctly |
| **Factory Naming** | ❌ Non-standard name | ✅ Standard `NewFactory()` |
| **Dependency Management** | ❌ Hidden from builder | ✅ Explicit in config |
| **Reusability** | ❌ Tied to receiver | ✅ Reusable by any component |
| **Version Management** | ❌ Not tracked | ✅ Tracked in go.mod |
| **Documentation** | ❌ Mixed with receiver | ✅ Separate README |
| **Testing** | ⚠️ Shared with receiver | ✅ Independent tests |
| **Discoverability** | ❌ Hidden in receiver | ✅ Listed in extension/ |

---

## Official OTel Component Structure

### How ALL 60+ Extensions Are Organized

**Check any official extension:**

1. **healthcheckextension:**
```
extension/healthcheckextension/
├── factory.go              # func NewFactory()
├── config.go
├── healthcheckextension.go
└── go.mod
```

2. **basicauthextension:**
```
extension/basicauthextension/
├── factory.go              # func NewFactory()
├── config.go
├── basicauthextension.go
└── go.mod
```

3. **datadogextension:**
```
extension/datadogextension/
├── factory.go              # func NewFactory()
├── config.go
├── datadogextension.go
└── go.mod
```

**Pattern:** 100% of extensions follow this structure.

---

## Real-World Example: What Happens in Code Review

### Script Approach

**Reviewer comments:**
```
❌ "Why is the extension in the receiver package?"
❌ "Why are you modifying generated code?"
❌ "Why isn't this in builder-config.yaml?"
❌ "This doesn't follow OTel standards"
❌ "Please restructure as a separate package"
```

**Result:** Request for changes before merge.

---

### Separate Package Approach

**Reviewer comments:**
```
✅ "Follows standard extension structure"
✅ "Properly integrated with builder"
✅ "Good separation of concerns"
✅ "Reusable across receivers"
✅ "Approved for merge"
```

**Result:** Immediate approval.

---

## Migration Effort Comparison

### Staying with Script

**Effort:** ✅ Zero (already done)

**Long-term cost:**
- ❌ Code review pushback if contributing to OTel
- ❌ Maintenance burden (script can break)
- ❌ Confusion for other developers
- ❌ Not reusable by Oracle receiver
- ❌ Technical debt

---

### Moving to Separate Package

**Effort:** 🔧 20 minutes

**Steps:**
1. Create `extension/querycache/` directory (1 min)
2. Move `cache_extension.go` → `extension/querycache/extension.go` (2 min)
3. Move `helpers/query_performance_cache.go` → `extension/querycache/cache.go` (2 min)
4. Create `extension/querycache/factory.go` with `NewFactory()` (3 min)
5. Create `extension/querycache/config.go` (2 min)
6. Create `extension/querycache/go.mod` (2 min)
7. Update imports in scraper.go (3 min)
8. Add to builder-config.yaml (1 min)
9. Remove script from Makefile (1 min)
10. Test build (3 min)

**Total:** ~20 minutes

**Long-term benefits:**
- ✅ Standards compliant
- ✅ No code review issues
- ✅ Reusable by Oracle receiver
- ✅ Clean architecture
- ✅ No technical debt

---

## Recommendation

### For Internal Use Only
**Keep script** if this is private/internal code that won't be contributed back to OTel.

**Pros:**
- Works now
- No restructuring needed

**Cons:**
- Non-standard
- Not reusable
- Technical debt

---

### For Contributing to OTel Contrib
**MUST use separate package** to meet contribution standards.

**Pros:**
- Standards compliant
- Passes code review
- Reusable
- Clean architecture

**Cons:**
- Requires 20 min restructuring

---

## Direct Answer to Your Question

> "Is using script aligns with otel standards? or separate package?"

**Answer:**

❌ **Script does NOT align with OTel standards** because:
1. Modifies generated code (violation)
2. Mixes component types in one package (violation)
3. Hidden from builder configuration (violation)
4. Non-standard factory naming (violation)
5. Not reusable (design problem)

✅ **Separate package FULLY aligns with OTel standards** because:
1. Follows official component structure
2. Proper separation of concerns
3. Builder-integrated
4. Standard factory naming
5. Reusable across components
6. Matches all 60+ existing extensions

---

## Conclusion

**If you're planning to:**
- ✅ **Contribute to OTel Contrib** → MUST use separate package
- ✅ **Make it reusable** → MUST use separate package
- ✅ **Follow best practices** → SHOULD use separate package
- ⚠️ **Keep it internal only** → Script is acceptable (but not ideal)

**My recommendation:** Invest 20 minutes to do it right with a separate package. It's the standard way and future-proof.
