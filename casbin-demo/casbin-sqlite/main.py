import casbin
import casbin_sqlalchemy_adapter

SQLALCHEMY_DATABASE_URI = "sqlite:///test.db"

# 从数据库加载casbin的policy
adapter = casbin_sqlalchemy_adapter.Adapter(SQLALCHEMY_DATABASE_URI)

enforcer = casbin.Enforcer("./model.conf", adapter)

policys = [['1', '/api/v1/user', 'post'],
           ['2', '/api/v1/user', 'put'],
           ['3', '/api/v1/user', 'delete']]

# 添加策略
[enforcer.add_policy(policy) for policy in policys]

result = [enforcer.enforce(*policy) for policy in policys]
print(result)