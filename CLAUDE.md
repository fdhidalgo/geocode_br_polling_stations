# Brazilian Polling Station Geocoding Project

## Project Goals

This project geocodes Brazilian polling stations from 2006-2022 elections through sophisticated string matching and machine learning techniques. The modernization effort focuses on:

1. **Code Quality**: Implement best practice coding approaches with consistent style
2. **Validation**: Add rigorous validation for all data cleaning steps
3. **Dependencies**: Reduce number of R package dependencies
4. **Reproducibility**: Ensure complete reproducibility using targets framework
5. **Documentation**: Comprehensive documentation of methodological decisions


## Claude Code Instructions

The current plan for modernization is in modernization_plan.md. Please use the github cli to interact with the github repo.  

When creating functions, please stop after each function and explain what you've created.  

After you add a function to  _targets.R or modify a function in _targets.R, check that the function works. 

Please ask follow up questions to add context and get additional clarity. 

## Systematic Commit Practices

This project follows systematic commit practices for research data analysis. Each commit should bundle:
- Implementation (focused change)
- Validation (data checks and pipeline tests)
- Documentation (methodology notes and decision logs)
- Issue thread link (detailed justification)

## Core Principles

### Data Validation Philosophy
- Every data transformation and merge must be validated
- Validation targets run automatically in pipeline
- Quality checks flag potential issues for review

### Issue-Driven Development
- Create GitHub issues for each analytical decision
- Document exploration in issue threads
- Link commits to relevant issues

### targets Pipeline Structure
- Atomic targets for each processing step

## Key Commands

### Development Workflow
```bash
# Check pipeline structure
tar_visnetwork()

# Run full pipeline
tar_make()

# Run specific target
tar_make(geocoded_polling_stations)

# Check outdated targets
tar_outdated()

# Read target results
tar_read(target_name)
```

### Code Style
```bash
# Format R code consistently
styler::style_dir("R/")

# Check for linting issues
lintr::lint_dir("R/")
```

## Project Structure

```
├── _targets.R              # Pipeline definition
├── R/
│   ├── functions_data.R    # Data processing functions
│   ├── functions_geocode.R # Geocoding functions
│   ├── functions_panel.R   # Panel creation functions
│   └── functions_validate.R # Validation functions
├── tests/                  # Unit tests for functions
├── data/
│   ├── raw/               # Original data files
│   └── processed/         # Intermediate processed data
├── output/                # Final geocoded datasets
└── docs/
    ├── methodology.md     # Detailed methodology
    └── codebook.md       # Variable definitions
```


## Development Guidelines

### Commit Message Format
```
Brief description - issue #XX

- Implementation: What changed
- Validation: How it's tested
- Documentation: What's documented
- Pipeline: tar_make() status

Closes #XX
```

### Branch Strategy
- `main`: Stable, reproducible state
- `feature/XX-description`: New features
- `fix/XX-description`: Bug fixes
- `explore/XX-description`: Experimental work

### Code Style Guidelines
- Use tidyverse style guide
- Explicit namespace references (package::function)
- Descriptive variable and function names
- Comprehensive function documentation

## Quality Assurance

### Before Each Commit
1. Run `tar_make()` successfully
2. Check `tar_visnetwork()` for pipeline structure
3. Run validation targets
4. Update documentation
5. Link to GitHub issue

