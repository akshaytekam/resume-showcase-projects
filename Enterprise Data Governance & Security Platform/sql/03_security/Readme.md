## Our Security Layer:

```text
03_security/
│
├── create_roles.sql
│        │
│        └── WHO?
│
├── grant_permissions.sql
│        │
│        └── WHAT OBJECTS?
│
├── masking_policies.sql
│        │
│        └── WHAT VALUES?
│
└── row_level_security.sql
         │
         └── WHICH ROWS?
```

So remember it as:

- RBAC = Who can access?
- GRANT = What can they access?
- Masking = What values can they see?
- RLS = Which records can they see?
