// SPDX-License-Identifier: GPL-2.0
/* BhavinCC: delay- and loss-sensitive TCP congestion control for CS 422. */
#include <linux/module.h>
#include <net/tcp.h>

#define BHAVIN_MIN_CWND 4U
#define BHAVIN_QUEUE_LOW 10U
#define BHAVIN_QUEUE_HIGH 25U

struct bhavin {
    u32 min_rtt_us;
    u32 srtt_ewma_us;
    u32 prior_delivered;
    u32 prior_lost;
    u64 prior_stamp_us;
    u64 rate_ewma_Bps;
    bool rate_improving;
    bool loss_seen;
};

static void bhavin_init(struct sock *sk)
{
    struct bhavin *ca = inet_csk_ca(sk);
    memset(ca, 0, sizeof(*ca));
    ca->min_rtt_us = ~0U;
}

static u32 bhavin_ssthresh(struct sock *sk)
{
    const struct tcp_sock *tp = tcp_sk(sk);
    return max(tp->snd_cwnd * 7 / 10, BHAVIN_MIN_CWND);
}

static void bhavin_cong_avoid(struct sock *sk, u32 ack, u32 acked)
{
    struct tcp_sock *tp = tcp_sk(sk);
    struct bhavin *ca = inet_csk_ca(sk);
    u32 rtt_us = max(tp->srtt_us >> 3, 1U);
    u32 queue_percent;

    if (!tcp_is_cwnd_limited(sk))
        return;
    ca->min_rtt_us = min(ca->min_rtt_us, rtt_us);
    ca->srtt_ewma_us = ca->srtt_ewma_us ?
        (7 * ca->srtt_ewma_us + rtt_us) / 8 : rtt_us;
    queue_percent = 100 * (ca->srtt_ewma_us - ca->min_rtt_us) /
                    max(ca->min_rtt_us, 1U);

    if (tcp_in_slow_start(tp)) {
        acked = tcp_slow_start(tp, acked);
        if (!acked)
            return;
    }
    if (ca->loss_seen) {
        tp->snd_cwnd = max(tp->snd_cwnd * 7 / 10, BHAVIN_MIN_CWND);
        ca->loss_seen = false;
    } else if (queue_percent > BHAVIN_QUEUE_HIGH && !ca->rate_improving) {
        if (tp->snd_cwnd > BHAVIN_MIN_CWND)
            tp->snd_cwnd--;
    } else if (queue_percent > BHAVIN_QUEUE_HIGH) {
        /* Throughput is still improving: hold cwnd instead of cutting it. */
    } else if (queue_percent < BHAVIN_QUEUE_LOW) {
        tcp_cong_avoid_ai(tp, tp->snd_cwnd, acked);
    } else {
        tcp_cong_avoid_ai(tp, max(tp->snd_cwnd * 2, 1U), acked);
    }
    tp->snd_cwnd = clamp(tp->snd_cwnd, BHAVIN_MIN_CWND, tp->snd_cwnd_clamp);
}

static void bhavin_pkts_acked(struct sock *sk, const struct ack_sample *sample)
{
    struct tcp_sock *tp = tcp_sk(sk);
    struct bhavin *ca = inet_csk_ca(sk);
    u64 now_us = tcp_clock_us();
    u32 delivered = tp->delivered - ca->prior_delivered;
    u64 interval = now_us - ca->prior_stamp_us;
    u64 rate;

    if (sample->rtt_us > 0)
        ca->min_rtt_us = min(ca->min_rtt_us, (u32)sample->rtt_us);
    ca->loss_seen = tp->lost > ca->prior_lost;
    if (ca->prior_stamp_us && interval && delivered) {
        rate = div64_u64((u64)delivered * tp->mss_cache * USEC_PER_SEC, interval);
        ca->rate_improving = !ca->rate_ewma_Bps || rate * 100 >= ca->rate_ewma_Bps * 102;
        ca->rate_ewma_Bps = ca->rate_ewma_Bps ?
            (7 * ca->rate_ewma_Bps + rate) / 8 : rate;
    }
    ca->prior_delivered = tp->delivered;
    ca->prior_lost = tp->lost;
    ca->prior_stamp_us = now_us;
}

static u32 bhavin_undo_cwnd(struct sock *sk)
{
    return max(tcp_sk(sk)->snd_cwnd, tcp_sk(sk)->prior_cwnd);
}

static struct tcp_congestion_ops bhavin_ops __read_mostly = {
    .flags = TCP_CONG_NON_RESTRICTED,
    .name = "bhavin",
    .owner = THIS_MODULE,
    .init = bhavin_init,
    .ssthresh = bhavin_ssthresh,
    .undo_cwnd = bhavin_undo_cwnd,
    .cong_avoid = bhavin_cong_avoid,
    .pkts_acked = bhavin_pkts_acked,
};

static int __init bhavin_register(void)
{
    BUILD_BUG_ON(sizeof(struct bhavin) > ICSK_CA_PRIV_SIZE);
    return tcp_register_congestion_control(&bhavin_ops);
}

static void __exit bhavin_unregister(void)
{
    tcp_unregister_congestion_control(&bhavin_ops);
}

module_init(bhavin_register);
module_exit(bhavin_unregister);
MODULE_AUTHOR("Bhavin Gupta");
MODULE_DESCRIPTION("CS 422 delay- and loss-sensitive TCP congestion control");
MODULE_LICENSE("GPL");
