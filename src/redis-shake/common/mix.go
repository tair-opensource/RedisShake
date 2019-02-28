package utils

import (
	"os"
	"path/filepath"

	"pkg/libs/log"

	"github.com/nightlyone/lockfile"
)

func WritePid(id string) (err error) {
	var lock lockfile.Lockfile
	lock, err = lockfile.New(id)
	if err != nil {
		return err
	}
	if err = lock.TryLock(); err != nil {
		return err
	}

	return nil
}

func WritePidById(id string) error {
	dir, _ := os.Getwd()
	pidfile := filepath.Join(dir, id) + ".pid"
	if err := WritePid(pidfile); err != nil {
		return err
	}
	return nil
}

func Welcome() {
	welcome :=
		`______________________________
\                             \           _         ______ |
 \                             \        /   \___-=O'/|O'/__|
  \  redis-shake, here we go !! \_______\          / | /    )
  /                             /        '/-==__ _/__|/__=-|  -GM
 /                             /         *             \ | |
/                             /                        (o)
------------------------------
`

	log.Warn("\n", welcome)
}

func Goodbye() {
	goodbye := `
                ##### | #####
Oh we finish ? # _ _ #|# _ _ #
               #      |      #
         |       ############
                     # #
  |                  # #
                    #   #
         |     |    #   #      |        |
  |  |             #     #               |
         | |   |   # .-. #         |
                   #( O )#    |    |     |
  |  ################. .###############  |
   ##  _ _|____|     ###     |_ __| _  ##
  #  |                                |  #
  #  |    |    |    |   |    |    |   |  #
   ######################################
                   #     #
                    #####
`

	log.Warn(goodbye)
}
