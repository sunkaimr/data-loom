package cmd

import (
	"fmt"
	"github.com/golang-jwt/jwt/v5"
	"github.com/spf13/cobra"
	"github.com/sunkaimr/data-loom/configs"
	"github.com/sunkaimr/data-loom/internal/pkg/common"
	"time"
)

var (
	user   string
	expire int
	secret string
)

func NewTokenCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "token",
		Short:   "token",
		Example: "data-loom token",
		PreRunE: CheckFlags,
		Run: func(cmd *cobra.Command, args []string) {
			claims := &common.Claims{
				UserName: user,
			}

			if expire == 0 {
				claims.ExpiresAt = jwt.NewNumericDate(time.Now().Add(time.Hour * 24 * 36500))
			} else {
				claims.ExpiresAt = jwt.NewNumericDate(time.Now().Add(time.Hour * time.Duration(expire)))
			}

			if token, err := common.GenerateToken(claims); err != nil {
				fmt.Println(err)
			} else {
				fmt.Println(token)
			}
		},
	}

	cmd.Flags().StringVarP(&user, "user", "", "admin",
		fmt.Sprintf("Sign a token for the <user>, default 'admin'"))
	cmd.Flags().IntVarP(&expire, "expire", "", 24,
		fmt.Sprintf("The expiration time of the token, 0 means never expires, default 24 hours"))
	cmd.Flags().StringVarP(&secret, "secret", "", "",
		fmt.Sprintf("the secret used for token generation, You can also specify it through a config file"))
	cmd.Flags().StringVarP(&config, "config", "", "./config.yaml",
		fmt.Sprintf("config file"))

	return cmd
}

func CheckFlags(cmd *cobra.Command, _ []string) error {
	if secret != "" {
		configs.C.Jwt.Secret = secret
		return nil
	}

	configs.Init(config)

	return nil
}
