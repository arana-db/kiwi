/*
 * Copyright (c) 2023-present, Arana/Kiwi Community.  All rights reserved.
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

package kiwi_test

import (
	"context"
	"log"
	"strconv"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redis/go-redis/v9"

	"github.com/OpenAtomFoundation/kiwi/tests/util"
)

var _ = Describe("Admin", Ordered, func() {
	var (
		ctx    = context.TODO()
		s      *util.Server
		client *redis.Client
	)

	// BeforeAll closures will run exactly once before any of the specs
	// within the Ordered container.
	BeforeAll(func() {
		config := util.GetConfPath(false, 0)

		s = util.StartServer(config, map[string]string{"port": strconv.Itoa(7777)}, true)
		Expect(s).NotTo(Equal(nil))
	})

	// AfterAll closures will run exactly once after the last spec has
	// finished running.
	AfterAll(func() {
		err := s.Close()
		if err != nil {
			log.Println("Close Server fail.", err.Error())
			return
		}
	})

	// When running each spec Ginkgo will first run the BeforeEach
	// closure and then the subject closure.Doing so ensures that
	// each spec has a pristine, correctly initialized, copy of the
	// shared variable.
	BeforeEach(func() {
		client = s.NewClient()
	})

	// nodes that run after the spec's subject(It).
	AfterEach(func() {
		err := client.Close()
		if err != nil {
			log.Println("Close client conn fail.", err.Error())
			return
		}
	})

	//TODO(dingxiaoshuai) Add more test cases.
	It("Cmd INFO", func() {
		log.Println("Cmd INFO Begin")
		Expect(client.Info(ctx).Val()).NotTo(Equal("FooBar"))
	})

	It("Cmd Shutdown", func() {
		Expect(client.Shutdown(ctx).Err()).NotTo(HaveOccurred())

		// kiwi does not support the Ping command right now
		// wait for 5 seconds and then ping server
		// time.Sleep(5 * time.Second)
		// Expect(client.Ping(ctx).Err()).To(HaveOccurred())

		// restart server
		config := util.GetConfPath(false, 0)
		s = util.StartServer(config, map[string]string{"port": strconv.Itoa(7777)}, true)
		Expect(s).NotTo(Equal(nil))

		// kiwi does not support the Ping command right now
		// wait for 5 seconds and then ping server
		// time.Sleep(5 * time.Second)
		// client = s.NewClient()
		// Expect(client.Ping(ctx).Err()).NotTo(HaveOccurred())
	})

	It("Cmd Select", func() {
		var outRangeNumber = 100

		r, e := client.Set(ctx, DefaultKey, DefaultValue, 0).Result()
		Expect(e).NotTo(HaveOccurred())
		Expect(r).To(Equal(OK))

		r, e = client.Get(ctx, DefaultKey).Result()
		Expect(e).NotTo(HaveOccurred())
		Expect(r).To(Equal(DefaultValue))

		rDo, eDo := client.Do(ctx, kCmdSelect, outRangeNumber).Result()
		Expect(eDo).To(MatchError(kInvalidIndex))

		r, e = client.Get(ctx, DefaultKey).Result()
		Expect(e).NotTo(HaveOccurred())
		Expect(r).To(Equal(DefaultValue))

		rDo, eDo = client.Do(ctx, kCmdSelect, 1).Result()
		Expect(eDo).NotTo(HaveOccurred())
		Expect(rDo).To(Equal(OK))

		r, e = client.Get(ctx, DefaultKey).Result()
		Expect(e).To(MatchError(redis.Nil))
		Expect(r).To(Equal(Nil))

		rDo, eDo = client.Do(ctx, kCmdSelect, 0).Result()
		Expect(eDo).NotTo(HaveOccurred())
		Expect(rDo).To(Equal(OK))

		rDel, eDel := client.Del(ctx, DefaultKey).Result()
		Expect(eDel).NotTo(HaveOccurred())
		Expect(rDel).To(Equal(int64(1)))
	})

	It("Cmd Config", func() {
		res := client.ConfigGet(ctx, "timeout")
		Expect(res.Err()).NotTo(HaveOccurred())
		Expect(res.Val()).To(Equal(map[string]string{"timeout": "0"}))

		res = client.ConfigGet(ctx, "daemonize")
		Expect(res.Err()).NotTo(HaveOccurred())
		Expect(res.Val()).To(Equal(map[string]string{"daemonize": "no"}))

		resSet := client.ConfigSet(ctx, "timeout", "60")
		Expect(resSet.Err()).NotTo(HaveOccurred())
		Expect(resSet.Val()).To(Equal("OK"))

		resSet = client.ConfigSet(ctx, "daemonize", "yes")
		Expect(resSet.Err()).To(MatchError("ERR Invalid Argument"))

		res = client.ConfigGet(ctx, "timeout")
		Expect(res.Err()).NotTo(HaveOccurred())
		Expect(res.Val()).To(Equal(map[string]string{"timeout": "60"}))

		res = client.ConfigGet(ctx, "time*")
		Expect(res.Err()).NotTo(HaveOccurred())
		Expect(res.Val()).To(Equal(map[string]string{"timeout": "60"}))
	})

	It("PING", func() {
		ping := client.Ping(ctx)
		Expect(ping.Err()).NotTo(HaveOccurred())
	})

	It("Cmd Debug", func() {
		// TODO: enable test after implementing DebugObject
		// res := client.DebugObject(ctx, "timeout")
		// Expect(res.Err()).NotTo(HaveOccurred())
		// Expect(res.Val()).To(Equal(map[string]string{"timeout": "0"}))
	})

	It("Cmd Sort", func() {
		size, err := client.LPush(ctx, "list", "1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(size).To(Equal(int64(1)))

		size, err = client.LPush(ctx, "list", "3").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(size).To(Equal(int64(2)))

		size, err = client.LPush(ctx, "list", "2").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(size).To(Equal(int64(3)))

		els, err := client.Sort(ctx, "list", &redis.Sort{
			Offset: 0,
			Count:  2,
			Order:  "ASC",
		}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(els).To(Equal([]string{"1", "2"}))

		del := client.Del(ctx, "list")
		Expect(del.Err()).NotTo(HaveOccurred())
	})

	It("should Sort and Get", Label("NonRedisEnterprise"), func() {
		size, err := client.LPush(ctx, "list", "1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(size).To(Equal(int64(1)))

		size, err = client.LPush(ctx, "list", "3").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(size).To(Equal(int64(2)))

		size, err = client.LPush(ctx, "list", "2").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(size).To(Equal(int64(3)))

		err = client.Set(ctx, "object_2", "value2", 0).Err()
		Expect(err).NotTo(HaveOccurred())

		{
			els, err := client.Sort(ctx, "list", &redis.Sort{
				Get: []string{"object_*"},
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(els).To(Equal([]string{"", "value2", ""}))
		}

		{
			els, err := client.SortInterfaces(ctx, "list", &redis.Sort{
				Get: []string{"object_*"},
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(els).To(Equal([]interface{}{nil, "value2", nil}))
		}
		del := client.Del(ctx, "list")
		Expect(del.Err()).NotTo(HaveOccurred())
	})

	It("should Sort and Store", Label("NonRedisEnterprise"), func() {
		size, err := client.LPush(ctx, "list", "1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(size).To(Equal(int64(1)))

		size, err = client.LPush(ctx, "list", "3").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(size).To(Equal(int64(2)))

		size, err = client.LPush(ctx, "list", "2").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(size).To(Equal(int64(3)))

		n, err := client.SortStore(ctx, "list", "list2", &redis.Sort{
			Offset: 0,
			Count:  2,
			Order:  "ASC",
		}).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(int64(2)))

		els, err := client.LRange(ctx, "list2", 0, -1).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(els).To(Equal([]string{"1", "2"}))

		del := client.Del(ctx, "list")
		Expect(del.Err()).NotTo(HaveOccurred())

		del2 := client.Del(ctx, "list2")
		Expect(del2.Err()).NotTo(HaveOccurred())
	})

	It("should monitor", Label("monitor"), func() {
		ress := make(chan string)
		client1 := s.NewClient()
		mn := client1.Monitor(ctx, ress)
		mn.Start()
		// Wait for the Redis server to be in monitoring mode.
		time.Sleep(100 * time.Millisecond)
		client.Set(ctx, "foo", "bar", 0)
		client.Set(ctx, "bar", "baz", 0)
		client.Set(ctx, "bap", 8, 0)
		client.Get(ctx, "bap")
		lst := []string{}
		for i := 0; i < 5; i++ {
			s := <-ress
			lst = append(lst, s)
		}
		mn.Stop()
		Expect(lst[0]).To(ContainSubstring("OK"))
		Expect(lst[2]).To(ContainSubstring(`"set foo bar"`))
		Expect(lst[3]).To(ContainSubstring(`"set bar baz"`))
		Expect(lst[4]).To(ContainSubstring(`"set bap 8"`))

		err := client1.Close()
		if err != nil {
			log.Println("Close monitor client conn fail.", err.Error())
			return
		}

	})
})
