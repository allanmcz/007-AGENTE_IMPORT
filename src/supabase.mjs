export async function login({ email, password, supabaseUrl, anonKey }) {
  try {
    const res = await fetch(`${supabaseUrl}/auth/v1/token?grant_type=password`, {
      method: "POST",
      headers: {
        apikey: anonKey,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ email, password }),
    });

    if (!res.ok) {
      const errorText = await res.text();
      throw new Error(`Auth Error (${res.status}): ${errorText}`);
    }

    const data = await res.json();
    return { jwt: data.access_token, userId: data.user.id };
  } catch (error) {
    throw new Error(`[Supabase] Erro no login: ${error.message}`);
  }
}

export async function rpc(name, params, { jwt, anonKey, supabaseUrl }) {
  try {
    const res = await fetch(`${supabaseUrl}/rest/v1/rpc/${name}`, {
      method: "POST",
      headers: {
        apikey: anonKey,
        Authorization: `Bearer ${jwt}`,
        "Content-Type": "application/json",
        Prefer: "return=representation",
      },
      body: JSON.stringify(params),
    });

    let body = null;
    if (res.status !== 204) {
      body = await res.json().catch(() => null);
    }

    return { status: res.status, body, error: !res.ok };
  } catch (err) {
    return { status: 500, error: true, body: { message: err.message } };
  }
}

export async function invokeFn(fnName, { method = "POST", body, headers = {}, jwt, anonKey, supabaseUrl }) {
  try {
    const defaultHeaders = {
      apikey: anonKey,
      Authorization: `Bearer ${jwt}`,
    };

    // FormData removes Content-Type and lets fetch calculate the boundary
    if (!(typeof window !== 'undefined' ? typeof FormData !== 'undefined' && body instanceof FormData : body && typeof body.append === 'function')) {
      if (typeof body === 'object' && body !== null) {
          defaultHeaders["Content-Type"] = "application/json";
          body = JSON.stringify(body);
      }
    }

    const res = await fetch(`${supabaseUrl}/functions/v1/${fnName}`, {
      method,
      headers: { ...defaultHeaders, ...headers },
      body,
    });

    let respBody = null;
    if (res.status !== 204) {
      respBody = await res.json().catch(() => null);
    }

    return { status: res.status, body: respBody, error: !res.ok };
  } catch (err) {
    return { status: 500, error: true, body: { message: err.message } };
  }
}

export async function query(table, queryString, { jwt, anonKey, supabaseUrl, prefer }) {
  try {
    const headers = {
      apikey: anonKey,
      Authorization: `Bearer ${jwt}`,
    };
    if (prefer) headers["Prefer"] = prefer;

    const res = await fetch(`${supabaseUrl}/rest/v1/${table}?${queryString}`, {
      method: "GET",
      headers,
    });

    if (!res.ok) {
      const errorText = await res.text();
      throw new Error(`Status ${res.status}: ${errorText}`);
    }

    return await res.json();
  } catch (error) {
    throw new Error(`[Supabase] Erro em select/head na tabela ${table}: ${error.message}`);
  }
}

export async function listEstabelecimentos(context) {
  return query("estabelecimentos", "select=id,cnpj,razao_social", context);
}
