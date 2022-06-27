package rest

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/square/go-jose.v2"
)

func TestLocalJWTAuthenticationE2E(t *testing.T) {
	const (
		testIssuer             = "test_issuer"
		testClientID           = "test_aud"
		testProviderName       = "test"
		testSubject            = "bilbo"
		testUsernamePrefix     = "test_prefix"
		testUsernameClaim      = "username"
		testUsernameClaimValue = "frodo"
	)

	testRSAKeypair, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	testRSAJWK := jose.JSONWebKey{
		Key:       testRSAKeypair.Public(),
		Use:       "sig",
		Algorithm: "RS256",
		KeyID:     "rsa",
	}

	runTest := func(preExistingUser bool, register bool, usernamePrefix, usernameClaim string) func(*testing.T) {
		return func(t *testing.T) {
			t.Logf("TEST: parameters: preExistingUser=%t register=%t usernamePrefix=%q usernameClaim=%q", preExistingUser, register, usernamePrefix, usernameClaim)
			base.SetUpTestLogging(t, base.LevelDebug, base.KeyAuth, base.KeyHTTP)

			var expectedUsername string
			switch {
			case usernamePrefix != "" && usernameClaim != "":
				expectedUsername = usernamePrefix + "_" + testUsernameClaimValue
			case usernamePrefix != "" && usernameClaim == "":
				expectedUsername = usernamePrefix + "_" + testSubject
			case usernamePrefix == "" && usernameClaim != "":
				expectedUsername = testUsernameClaimValue
			case usernamePrefix == "" && usernameClaim == "":
				expectedUsername = testProviderName + "_" + testSubject
			}
			t.Logf("TEST: expected username %q", expectedUsername)

			providers := auth.LocalJWTConfig{
				testProviderName: &auth.LocalJWTAuthProvider{
					JWTConfigCommon: auth.JWTConfigCommon{
						Issuer:        testIssuer,
						ClientID:      base.StringPtr(testClientID),
						Register:      register,
						UsernameClaim: usernameClaim,
						UserPrefix:    usernamePrefix,
					},
					Algorithms: auth.JWTAlgList{"RS256"},
					Keys:       []jose.JSONWebKey{testRSAJWK},
				},
			}
			restTesterConfig := RestTesterConfig{DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{LocalJWTConfig: providers}}}
			restTester := NewRestTester(t, &restTesterConfig)
			require.NoError(t, restTester.SetAdminParty(false))
			defer restTester.Close()

			mockSyncGateway := httptest.NewServer(restTester.TestPublicHandler())
			defer mockSyncGateway.Close()
			mockSyncGatewayURL := mockSyncGateway.URL

			authenticator := restTester.ServerContext().Database("db").Authenticator(base.TestCtx(t))
			if preExistingUser {
				user, err := authenticator.RegisterNewUser(expectedUsername, "")
				require.NoError(t, err, "Failed to create test user")
				t.Logf("TEST: created user %q", user.Name())
			}

			claims := map[string]interface{}{
				"iss": testIssuer,
				"sub": testSubject,
				"aud": []string{testClientID},
			}
			if usernameClaim != "" {
				claims[testUsernameClaim] = testUsernameClaimValue
			}
			token := auth.CreateTestJWT(t, jose.RS256, testRSAKeypair, auth.JWTHeaders{
				"kid": testRSAJWK.KeyID,
			}, claims)

			req, err := http.NewRequest(http.MethodPost, mockSyncGatewayURL+"/db/_session", bytes.NewBufferString("{}"))
			require.NoError(t, err)

			req.Header.Set("Authorization", BearerToken+" "+token)

			res, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer res.Body.Close()

			if !preExistingUser && !register {
				require.Equal(t, http.StatusUnauthorized, res.StatusCode)
				return
			}

			require.Equal(t, http.StatusOK, res.StatusCode)

			user, err := authenticator.GetUser(expectedUsername)
			require.NoError(t, err)
			require.NotNil(t, user, "user was nil")
			assert.Equal(t, testIssuer, user.OIDCIssuer())
		}
	}

	for _, register := range []bool{true, false} {
		for _, preExisting := range []bool{true, false} {
			for _, usernamePrefix := range []string{"", testUsernamePrefix} {
				for _, usernameClaim := range []string{"", testUsernameClaim} {
					var testNameParts []string
					if register {
						testNameParts = append(testNameParts, "register")
					}
					if preExisting {
						testNameParts = append(testNameParts, "pre-existing user")
					}
					if usernamePrefix != "" {
						testNameParts = append(testNameParts, "username prefix")
					}
					if usernameClaim != "" {
						testNameParts = append(testNameParts, "username claim")
					}
					if len(testNameParts) == 0 {
						testNameParts = []string{"base"}
					}
					t.Run(strings.Join(testNameParts, "__"), runTest(preExisting, register, usernamePrefix, usernameClaim))
				}
			}
		}
	}
}

// Tests a subset of the cases covered by auth.TestJWTVerifyToken.
func TestLocalJWTAuthenticationNegative(t *testing.T) {
	testRSAKeypair, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	testRSAJWK := jose.JSONWebKey{
		Key:       testRSAKeypair.Public(),
		Use:       "sig",
		Algorithm: "RS256",
		KeyID:     "rsa",
	}

	testECKeypair, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	testECJWK := jose.JSONWebKey{
		Key:       testECKeypair.Public(),
		Use:       "sig",
		Algorithm: "ES256",
		KeyID:     "ec",
	}

	const (
		testProviderName = "test"
		testIssuer       = "testIssuer"
		testSubject      = "bilbo"
		testClientID     = "testAud"
	)

	common := auth.JWTConfigCommon{
		Issuer:   testIssuer,
		ClientID: base.StringPtr(testClientID),
	}
	baseProvider := auth.LocalJWTAuthProvider{
		JWTConfigCommon: common,
		Algorithms:      auth.JWTAlgList{"RS256", "ES256"},
		Keys:            []jose.JSONWebKey{testRSAJWK, testECJWK},
	}

	runTest := func(provider *auth.LocalJWTAuthProvider, token string, createUserName string, expectedStatus int) func(*testing.T) {
		return func(t *testing.T) {
			base.SetUpTestLogging(t, base.LevelDebug, base.KeyAuth, base.KeyHTTP)
			restTesterConfig := RestTesterConfig{DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{LocalJWTConfig: auth.LocalJWTConfig{
				testProviderName: provider,
			}}}}
			restTester := NewRestTester(t, &restTesterConfig)
			require.NoError(t, restTester.SetAdminParty(false))
			defer restTester.Close()

			mockSyncGateway := httptest.NewServer(restTester.TestPublicHandler())
			defer mockSyncGateway.Close()
			mockSyncGatewayURL := mockSyncGateway.URL

			if createUserName != "" {
				authn := restTester.GetDatabase().Authenticator(base.TestCtx(t))
				_, err = authn.RegisterNewUser(createUserName, "test@sgwdev.com")
				require.NoError(t, err, "Failed to register test user %s", createUserName)
			}

			req, err := http.NewRequest(http.MethodPost, mockSyncGatewayURL+"/db/_session", bytes.NewBufferString("{}"))
			require.NoError(t, err)

			req.Header.Set("Authorization", BearerToken+" "+token)

			res, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer res.Body.Close()

			assert.Equal(t, expectedStatus, res.StatusCode)
		}
	}

	testUsername := testProviderName + "_" + testSubject

	t.Run("valid - RSA", runTest(&baseProvider, auth.CreateTestJWT(t, jose.RS256, testRSAKeypair, auth.JWTHeaders{
		"alg": jose.RS256,
		"kid": testRSAJWK.KeyID,
	}, map[string]interface{}{
		"iss": testIssuer,
		"aud": []string{testClientID},
		"sub": testSubject,
	}), testUsername, http.StatusOK))

	t.Run("valid - EC", runTest(&baseProvider, auth.CreateTestJWT(t, jose.ES256, testECKeypair, auth.JWTHeaders{
		"alg": jose.ES256,
		"kid": testECJWK.KeyID,
	}, map[string]interface{}{
		"iss": testIssuer,
		"aud": []string{testClientID},
		"sub": testSubject,
	}), testUsername, http.StatusOK))

	t.Run("garbage", runTest(&baseProvider, "garbage", testUsername, http.StatusUnauthorized))

	// header: alg=none
	t.Run("valid JWT with alg none", runTest(
		&baseProvider,
		`eyJhbGciOiJub25lIn0.eyJhdWQiOlsidGVzdEF1ZCJdLCJpc3MiOiJ0ZXN0SXNzIn0.`,
		testUsername,
		http.StatusUnauthorized))
	// header: alg=HS256
	t.Run("valid JWT with alg HS256", runTest(&baseProvider,
		`eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOlsidGVzdEF1ZCJdLCJpc3MiOiJ0ZXN0SXNzIn0.gbdmOrzJ2CT01ABybPN-_dwXwv8_8iMEj4HNPtBqQjI`,
		testUsername,
		http.StatusUnauthorized))
}
